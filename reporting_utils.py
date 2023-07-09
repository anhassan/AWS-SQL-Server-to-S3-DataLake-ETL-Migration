import sys
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from awsglue.job import Job
import time
import pandas as pd

spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context.getOrCreate())
spark = glue_context.spark_session
job = Job(glue_context)     

class ReportingUtils(object):
    
    def __init__(self,utils):
        self.utils = utils
        self.s3_client = boto3.client("s3")
        self.s3_resource = boto3.resource("s3")
        self.glue_client = boto3.client("glue")
        
    
    def _get_all_tables(self,bucket,ignore_val= "_$folder"):
        all_objs = self.s3_client.list_objects(Bucket=bucket)
        all_files = [dir["Key"] for dir in all_objs["Contents"]]
        all_tables = list(set([file[0:file.find("/")] for file in all_files if ignore_val not in file]))
        return all_tables
    

    def _get_table_size(self,bucket,table_name):
        total_size = 0

        for file in self.s3_resource.Bucket(bucket).objects.filter(Prefix=table_name):
            total_size += file.size
        return total_size
    

    def get_all_table_sizes(self,bucket,tables_list):
        all_tables_meta = [[table,self._get_table_size(bucket,table)]for table in tables_list]
        df = pd.DataFrame(all_tables_meta,columns=["TableName","TableSize"])
        return spark.createDataFrame(df)
    

    def _get_job_execution_time(self,table_name):

        try:
            runs = self.glue_client.get_job_runs(JobName=table_name)
            for run in runs["JobRuns"]:
                if run["JobRunState"] == "SUCCEEDED":
                    return run["ExecutionTime"]
            return -1
        except Exception as error:
            return -1
        

    def get_all_jobs_execution_times(self,all_tables):
        all_tables_meta = [[table,self._get_job_execution_time(table)] for table in all_tables if self._get_job_execution_time(table) > 0]
        df = pd.DataFrame(all_tables_meta,columns=["TableName","IngestionTime"])
        return spark.createDataFrame(df)
    
    
    def get_onprem_datalake_counts(self,bucket_name,onprem_database_name,table_names):
        all_counts = []
        onprem_table_names = [table_name.replace("_",".") for table_name in table_names]
        for table_index,onprem_table in enumerate(onprem_table_names):
            try:
                onprem_count = self.utils.read_from_onprem(onprem_database_name,onprem_table).count()
                datalake_count = self.utils.read_from_datalake(bucket_name,table_names[table_index]).count()
                all_counts.append([onprem_table, onprem_count, table_names[table_index],datalake_count])
            except Exception as error:
                print("Skipped Table : {}".format(table_names[table_index]))
        df = pd.DataFrame(all_counts,columns=["OnpremTableName","OnpremRowCounts","TableName","TableRowCounts"])
        return spark.createDataFrame(df)
    
    
    def generate_recon_report(self,bucket_name,onprem_database_name):
        all_tables = self._get_all_tables(bucket_name)
        
        df_sizes = self.get_all_table_sizes(bucket_name,all_tables)
        df_times = self.get_all_jobs_execution_times(all_tables)
        df_counts = self.get_onprem_datalake_counts(bucket_name,onprem_database_name,all_tables)
        
        df_report = df_counts.join(df_times,["TableName"])\
                             .join(df_sizes,["TableName"])

        self.utils.write_to_datalake(df_report,bucket_name,"recon_report")

        return df_report
    
    