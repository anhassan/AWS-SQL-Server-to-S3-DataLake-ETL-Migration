import sys
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from datetime import datetime
import time
import math
from pyspark.sql import DataFrame
import numpy as np
import decimal
import builtins as bi
#from delta import *


spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context.getOrCreate())
spark = glue_context.spark_session
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
job = Job(glue_context)

class BaseUtils(object):
    
    def __init__(self):
            self.s3_resource = boto3.resource("s3")
    
    def find_s3_folders(self,s3_bucket,filter_path="datalake",ignore_pattern="$folder"):
            s3_bucket = self.s3_resource.Bucket(s3_bucket)
            all_folders = []
            for obj in s3_bucket.objects.all():
                folder = obj.key[0:obj.key.rfind("/")]
                if folder not in all_folders and ignore_pattern not in folder and filter_path in folder :
                    all_folders.append(folder)
            if filter_path in all_folders : all_folders.remove(filter_path)
            return all_folders

    def generate_batchs(self,batch_list,batch_size):
    		output_batch = []
    		num_splits = math.ceil(decimal.Decimal(len(batch_list))/decimal.Decimal(batch_size))
    		indices_arr = np.array(list(range(len(batch_list))))
    		indices_arr_batchs = np.array_split(indices_arr,num_splits)
    		indices_batchs = [list(batch) for batch in indices_arr_batchs]
    		for indices_batch in indices_batchs:
    		    output_batch.append(batch_list[bi.min(indices_batch):bi.max(indices_batch)+1])
    		return output_batch
    
    def rename_cols_csv(self,df):
            new_df = df
            prev_cols = list(df.columns)
            
            parquet_invalid_chars = ",;{}()\n\t="
            delim_char = "*"
            remove_chars = str.maketrans(parquet_invalid_chars, delim_char*len(parquet_invalid_chars))
            
            new_cols = [col.strip().replace(" ", "_").translate(remove_chars).replace(delim_char,"") for col in prev_cols]
            for index,prev_col in enumerate(prev_cols):
                new_df = new_df.withColumnRenamed(prev_col,new_cols[index])
            return new_df

class Utils(object):
    
    def __init__(self):
        self.s3_resource = boto3.resource("s3")
        self.crawler = CrawlerUtils()
        self.base_utils = BaseUtils()


    def append_to_delta(self,df,bucket_name,table_path):
    	df.write\
          .format("delta")\
          .mode("append")\
          .save("s3://{}/datalake/{}".format(bucket_name,table_path))


    def read_from_onprem(self,database,table,query=""):
    
        host_ip, host_port = "18.130.16.83", "1433"
        host_user, host_password = "admin","@dd0@dd0"
        table_statement = "({}) as temp_table".format(query) if query else table
        
        try:
            
            table_df = spark.read\
                            .format("jdbc")\
                            .option("url","jdbc:sqlserver://{}:{};databaseName={};".format(host_ip,host_port,database))\
                            .option("dbtable",table_statement)\
                            .option("user",host_user)\
                            .option("password",host_password)\
                            .load()
            return table_df
        
        except Exception as error:
            
            generic_error_statement = "FAILURE - Error : {} while Reading Table : {} from Database : {}".format(error,table,database)
            error_statement = "{} with Query : {}".format(generic_error_statement,query) if query else generic_error_statement
            print(error_statement)


    def read_from_onprem_conn(self,database,table,query=""):

	    transformed_table_name = "{}_{}".format(database.lower(), table.replace(".","_").lower())

	    table_dyf = glue_context.create_dynamic_frame.from_catalog(
	             database="nis-onprem-database",
	             table_name=transformed_table_name)

	    table_df = table_dyf.toDF()

	    return table_df
    


    def read_from_datalake(self,bucket_name,table_path,query=""):
    
        table_name = table_path[table_path.rfind("/")+1:] if "/" in table_path else table_path
        
        try:
            
            table_df = spark.read.parquet("s3://{}/datalake/{}".format(bucket_name,table_path))
            if query:
                table_df.createOrReplaceTempView(table_name)
                table_df = spark.sql(query)
                spark.catalog.dropTempView(table_name)
            return table_df
        
        except Exception as error:
            
            generic_error_statement = "FAILURE - Error : {} while Reading Table : {}".format(error,table_name)
            error_statement = "{} with Query : {}".format(generic_error_statement,query) if query else generic_error_statement
            print(error_statement)
            
    
    def read_datalake_csv(self,bucket_name,file_path,ddl=""):
            if ddl:

                ddl_components = ddl.split("\n")
                table_name = [element.replace("CREATE TABLE", "").replace("(","").strip() for element in ddl_components if "CREATE TABLE" in element][0]

                try:
                    spark.sql("DROP TABLE {}".format(table_name))
                except Exception as error:
                    pass

                data_source = """
                                USING csv
                                OPTIONS (
                                    path 's3://{}/{}',
                                    header 'true',
                                    multiLine 'true'
                            )""".format(bucket_name,file_path)
                
                complete_ddl = ddl + data_source

                
                spark.sql(complete_ddl)
                df = spark.sql("SELECT * FROM {}".format(table_name))
                spark.sql("DROP TABLE {}".format(table_name))
                
                
            else:
                
                df = spark.read\
                        .option("header",True)\
                        .option("inferSchema",True)\
                        .option("multiLine",True)\
                        .csv("s3://{}/{}".format(bucket_name,file_path))
            return df
    
    def read_incremental_from_datalake(self,bucket_name,table_path,query=""):
	    table_name = table_path[table_path.rfind("/")+1:] if "/" in table_path else table_path
	    try:
	        table_df = spark.read\
	                        .format("delta")\
	                        .load("s3://{}/datalake/{}".format(bucket_name,table_path))
	        if query:
	            table_df.createOrReplaceTempView(table_name)
	            table_df = spark.sql(query)
	            spark.catalog.dropTempView(table_name)
	        return table_df
	    except Exception as error:
	        generic_error_statement = "FAILURE - Error : {} while Reading Table : {}".format(error,table_name)
	        error_statement = "{} with Query : {}".format(generic_error_statement,query) if query else generic_error_statement
	        print(error_statement)
	        return -1
    
    def ingest_csv_datalake(self,bucket_name,file_path,table_name,ddl=""):
        df = self.read_datalake_csv(bucket_name,file_path,ddl)
        parquet_df = self.base_utils.rename_cols_csv(df)
        self.write_to_datalake(parquet_df,bucket_name,table_name)
        
    
    def write_to_datalake(self,df,bucket_name,table_path):
        table_name = table_path[table_path.rfind("/")+1:] if "/" in table_path else table_path
        try:
            df.write\
              .mode("overwrite")\
              .parquet("s3://{}/datalake/{}".format(bucket_name,table_path))
            self.crawler.start_crawler_table(bucket_name,table_path)
            print("Successfully wrote Table : {}.....".format(table_name))
        except Exception as error:
            print("Persisting Table : {} in Bucket  : {} Failed with Error : {}".format(bucket_name,table_name,error))


    def write_incremental_to_datalake(self,df,bucket_name,table_path,primary_key,updates=False):
	    table_name = table_path[table_path.rfind("/")+1:] if "/" in table_path else table_path
	    prev_df = read_incremental_from_datalake(bucket_name,table_path)
	    
	    try:
	        if isinstance(prev_df,DataFrame):

	        	max_pk = prev_df.select(primary_key).agg({primary_key:"max"}).collect()[0]["max({})".format(primary_key)]
		        df_incremental = df.where(col(primary_key) > max_pk)

	        	if updates:
                            target_table = DeltaTable.forPath(spark,"s3://{}/{}".format(bucket_name,table_path))
                            target_table.alias("target")\
                            .merge(
                                df.alias("incremental"),
                                "target.{} = incremental.{}".format(primary_key,primary_key))\
                            .whenMatchedUpdateAll()\
                            .whenNotMatchedInsertAll()\
                            .execute()

                            print("Successfully updated Table : {} ......".format(table_name))
		        else:

                             self.append_to_delta(df_incremental,bucket_name,table_path)
                             print("Successfully Incremented Table : {}.....".format(table_name))

	        else:
	            self.append_to_delta(df,bucket_name,table_path)
	            print("Successfully wrote Table : {}.....".format(table_name))

	        self.crawler.start_crawler_table(bucket_name,table_path)
	        
	    except Exception as error:
	        print("Persisting Table : {} in Bucket  : {} Failed with Error : {}".format(bucket_name,table_name,error))
	        return -1
    
    

class CrawlerUtils(object):
    
    def __init__(self):
        self.glue_client = boto3.client("glue")
        self.utils = BaseUtils()
    
    def create_crawler(self,bucket_name):
        try:
            s3_targets = [{"Path" : "s3://{}/datalake/{}".format(bucket_name,folder)} for folder in self.utils.find_s3_folders(bucket_name)]
            self.glue_client.create_crawler(
                Name="{}-crawler".format(bucket_name),
                Role="AWSGlueServiceRoleDefault",
                DatabaseName="crisis-24",
                Targets={
                    "S3Targets" : s3_targets
                })
        except Exception as error:
            print("Crawler Creation for Targets : {} Failed with Error : {}".format(s3_targets,error))

    def create_crawler_table(self,bucket_name,table_path):
        try:
            table_name = table_path[table_path.rfind("/")+1:] if "/" in table_path else table_path
            s3_target = [{"Path" : "s3://{}/datalake/{}".format(bucket_name,table_path)} ]
            self.glue_client.create_crawler(
                Name="{}-{}-crawler".format(bucket_name,table_name),
                Role="AWSGlueServiceRoleDefault",
                DatabaseName="crisis-24",
                Targets={
                    "S3Targets" : s3_target
                })
        except Exception as error:
            print("Crawler Creation for Targets : {} Failed with Error : {}".format(s3_target,error))
    
    def get_crawler_status(self,bucket_name):
        try:
            crawler_status = self.glue_client.get_crawler(Name="{}-crawler".format(bucket_name))["Crawler"]["State"]
            return crawler_status
        except Exception as error:
            print("Getting status of crawler : {}-crawler failed....".format(bucket_name))
            return "FAILED"

    def update_crawler_targets(self,bucket_name,updated_s3_targets):
        try:
            updated_s3_target_paths = [{"Path" : s3_target} for s3_target in updated_s3_targets]
            self.glue_client.update_crawler(
                Name="{}-crawler".format(bucket_name),
                DatabaseName="crisis-24",
                Targets={
                    "S3Targets" : updated_s3_target_paths
                })
            print("Added data sources : {}".format(updated_s3_targets))
        except Exception as error:
            print("Crawler Updation for Targets : {} Failed with Error : {}".format(updated_s3_targets,error))
        
    def start_crawler(self,bucket_name):
        glue_crawlers = self.glue_client.list_crawlers()["CrawlerNames"]
        crawler_name = "{}-crawler".format(bucket_name)
        if crawler_name not in glue_crawlers:
            self.create_crawler(bucket_name)
            self.glue_client.start_crawler(Name=crawler_name)
        else:
            if "READY" in self.get_crawler_status(bucket_name):
                all_s3_targets = ["s3://{}/{}".format(bucket_name,folder) for folder in self.utils.find_s3_folders(bucket_name)]
                present_s3_targets = [target["Path"] for target in self.glue_client.get_crawler(Name=crawler_name)["Crawler"]["Targets"]["S3Targets"]]
                additional_s3_targets = list(set(all_s3_targets) ^ set(present_s3_targets))
                if additional_s3_targets:
                    self.update_crawler_targets(bucket_name,all_s3_targets)
                self.glue_client.start_crawler(Name=crawler_name)

    def start_crawler_table(self,bucket_name,table_path):
    	table_name = table_path[table_path.rfind("/")+1:] if "/" in table_path else table_path
    	glue_crawlers = self.glue_client.list_crawlers()["CrawlerNames"]
    	crawler_name = "{}-{}-crawler".format(bucket_name,table_name)
    	if crawler_name not in glue_crawlers:
    		self.create_crawler_table(bucket_name,table_path)
    		self.glue_client.start_crawler(Name=crawler_name)
    	else:
    		if "READY" in self.get_crawler_status("{}-{}".format(bucket_name,table_name)):
    			self.glue_client.start_crawler(Name=crawler_name)



class RunUtils(object):
    
    def __init__(self):
        self.crawler = CrawlerUtils()
        self.glue_client = boto3.client("glue")
        self.metastore_client = OperationalMetadataUtils("Lake")
    
    
    def wait_for_completion(self,job_name,job_run_id):
        job_state = "UNKNOWN"
        while job_state != "SUCCEEDED" and job_state != "FAILED":
            time.sleep(0.5)
            try:
                job_state = self.glue_client.get_job_run(JobName=job_name,RunId=job_run_id)["JobRun"]["JobRunState"]
            except Exception as error:
                print("Error on wait for completion- Error log : {}".format(error))
                job_state = "FAILED"
        return job_state
    
    def log_metadata(self,meta_records):
        self.metastore_client.insert_metadata(meta_records)
        self.crawler.start_crawler_table(self.metastore_client.metastore_bucket,self.metastore_client.metastore_table)
        
    
    
    def run_series(self,job_names,args=[]):
        
        args = len(job_names)*[{}] if len(args) == 0 else args
        all_jobs = [job["Name"] for page in self.glue_client.get_paginator("get_jobs").paginate() for job in page["Jobs"]]
        meta_records = []
        
        for index,job_name in enumerate(job_names):
            if job_name in all_jobs:
                
                job_arguments = args[index]
                job_run_id = self.glue_client.start_job_run(JobName=job_name,Arguments=job_arguments)["JobRunId"]
                job_start_time = datetime.now()
                print("Job Name : {} with Arguments : {} Started...".format(job_name,job_arguments))
                job_state = self.wait_for_completion(job_name,job_run_id)
                job_end_time = datetime.now()
                job_duration = math.ceil((job_end_time - job_start_time).total_seconds())
                print("Job Name : {} with Arguments : {} completed with Status : {}".format(job_name,job_arguments,job_state))
                
                if job_state == "FAILED":
                    
                    meta_records.append([job_name,str(job_arguments),"FAILURE",job_start_time,job_end_time,job_duration])
                    
                    for sub_index,sub_job_name in enumerate(job_names[index+1:]):
                        start_time = datetime.now()
                        meta_records.append([sub_job_name,str(args[sub_index]),"SUSPENDED",start_time,start_time,0])
                    
                    self.log_metadata(meta_records)
                    raise Exception("Run for Job Name : {} with Arguments : {} Failed".format(job_name,job_arguments))
                    
                else:
                    meta_records.append([job_name,str(job_arguments),"SUCCESS",job_start_time,job_end_time,job_duration])
            else:
                self.log_metadata(meta_records)
                raise Exception("No Job with job name : {} exists in Glue".format(job_name))
        
        self.log_metadata(meta_records)
                
    
    def run_parallel(self,job_names,args=[]):
        
        args = len(job_names)*[{}] if len(args) == 0 else args
        all_jobs = [job["Name"] for page in self.glue_client.get_paginator("get_jobs").paginate() for job in page["Jobs"]]
        
        job_run_ids, job_run_states, all_run_jobs, job_arguments = [],[],[],[]
        job_start_times, job_end_times  = [],[]
        meta_records = []
        
        for index,job_name in enumerate(job_names):
            if job_name in job_names:
                
                job_run_id = self.glue_client.start_job_run(JobName=job_name,Arguments=args[index])["JobRunId"]
                job_arguments.append(args[index])
                job_start_times.append(datetime.now())
                job_run_ids.append(job_run_id)
                all_run_jobs.append(job_name)
                print("Ran Job Name : {} with Arguments : {} Started...".format(job_name,args[index]))
                
            else:
                print("Job Name : {} does not exist therefore skipping it....".format(job_name))
                
        job_run_states += len(all_run_jobs)*[False]
        job_end_times += len(job_start_times)*[datetime.now()]
        completed_jobs = 0
        
        while completed_jobs < len(all_run_jobs):
            time.sleep(0.5)
            
            for index,job_name in enumerate(all_run_jobs):
                
                job_state = self.glue_client.get_job_run(JobName=job_name,RunId=job_run_ids[index])["JobRun"]["JobRunState"]
                
                if job_state == "SUCCEEDED" or job_state == "FAILED":
                    if not job_run_states[index]:
                        
                        job_args = job_arguments[index]
                        job_end_time = datetime.now()
                        job_start_time = job_start_times[index]
                        job_duration = math.ceil((job_end_time - job_start_time).total_seconds())
                        print("Job Name : {} completed with Status : {} ".format(job_name,job_state))
                        job_run_states[index] = True
                        
                        if job_state == "FAILED":
                            meta_records.append([job_name,str(job_args),"FAILURE",job_start_time,job_end_time,job_duration])
                            print("Job Name : {} with Arguments : {} failed...".format(job_name,job_args))
                        else:
                            meta_records.append([job_name,str(job_args),"SUCCESS",job_start_time,job_end_time,job_duration])
                            print("Job Name : {} with Arguments : {} succeeded ".format(job_name,job_args))
                            
            completed_jobs = len([job_state for job_state in job_run_states if job_state])
        
        self.log_metadata(meta_records)
    
    
    
class OperationalMetadataUtils(object):
    
    def __init__(self,metastore_type,rds_credentials={}):
        self.metastore_type = metastore_type
        self.metastore_bucket = "c24-data-extract"
        self.metastore_table = "operational_metadata"
        self.rds_credentials = rds_credentials
        
    
    def create_metadata_records(self,records):
        records_schema = StructType([
            StructField("job_name",StringType(),False),
            StructField("job_args",StringType(),True),
            StructField("job_status",StringType(),True),
            StructField("job_start_time",TimestampType(),True),
            StructField("job_end_time",TimestampType(),True),
            StructField("job_duration",IntegerType(),True)
        ])
        
        records_df = spark.createDataFrame(data=records,schema=records_schema)
        return records_df
    
    
    def insert_meta_lake(self,records_df):
        records_df.write\
                  .mode("append")\
                  .parquet("s3://{}/datalake/{}".format(self.metastore_bucket,self.metastore_table))
                
    
    
    def insert_meta_rds(self,records_df):
        records_df.write\
                  .mode("append")\
                  .options(
                         url=self.rds_credentials["url"],
                         dbtable=self.rds_credentials["table_name"],
                         user=self.rds_credentials["user"],
                         password=self.rds_credentials["password"],
                         driver='oracle.jdbc.OracleDriver')\
                  .save()
             
    
    def insert_metadata(self,records):
        records_df = self.create_metadata_records(records)
        if self.metastore_type == "Lake":
            self.insert_meta_lake(records_df)
        if self.metastore_type == "Rds":
            self.insert_meta_rds(records_df)
            
    
    