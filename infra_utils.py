import sys
import boto3
from botocore.config import Config
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from awsglue.job import Job
import time

spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context.getOrCreate())
spark = glue_context.spark_session
job = Job(glue_context)

class SetUpInfraUtils(object):
    
    def __init__(self):
        self.glue_client = boto3.client("glue", config=Config(connect_timeout=5, read_timeout=60, retries={'max_attempts': 20}))
        self.s3_client = boto3.client("s3")
        self.s3_resource = boto3.resource("s3")
        
    
    def _get_all_buckets(self):
        all_buckets_obj = self.s3_client.list_buckets()
        all_buckets = []
        if "Buckets" in all_buckets_obj.keys():
            all_buckets += [obj["Name"] for obj in all_buckets_obj["Buckets"]]
        return all_buckets
    
    
    def create_bucket(self,bucket_name,bucket_location):
        try:
            if bucket_name not in self._get_all_buckets():
                self.s3_resource.create_bucket(Bucket=bucket_name,CreateBucketConfiguration={
                "LocationConstraint":bucket_location})
        except Exception as error:
            print("Bucket Creation of Bucket : {} failed with following error : {}".format(bucket_name,error))
            
    
    def _get_all_databases(self):
        all_databases_obj = self.glue_client.get_databases()
        all_databases = []
        if "DatabaseList" in all_databases_obj.keys():
            all_databases += [obj["Name"] for obj in all_databases_obj["DatabaseList"]]
        return all_databases
    
    
    def create_glue_database(self,database_name):
        try:
            if database_name not in self._get_all_databases():
                self.glue_client.create_database(
                    DatabaseInput={
                    "Name":database_name})
        except Exception as error:
            print("Glue Database Creation of Database : {} failed with following error : {}".format(database_name,error))
            
    
    def _crawler_present(self,crawler_name):
        crawler_present = False
        all_crawlers = self.glue_client.list_crawlers()
        if "CrawlerNames" in all_crawlers.keys():
            crawler_present = target_crawler in all_crawlers["CrawlerNames"]
        return crawler_present
    
    
    def _get_crawler_status(self,crawler_name):
        try:
            crawler_status = self.glue_client.get_crawler(Name=crawler_name)["Crawler"]["State"]
            return crawler_status
        except Exception as error:
            print("Getting status of crawler : {} failed....".format(crawler_name))
            return "FAILED"
        
        
    def load_onprem_dependencies(self, conn_name , crawler_name = "nis-onprem-crawler",database_name = "nis-onprem-database"):
        if not _crawler_present(crawler_name):
            self.create_glue_database(database_name)
            self.glue_client.create_crawler(
                Name = crawler_name,
                Role = "AWSGlueServiceRoleDefault",
                DatabaseName = database_name,
                Targets = {
                        'JdbcTargets' : [
                            {
                                'ConnectionName': conn_name
                                }
                        ]
                    }
            )
            self.glue_client.start_crawler(Name=crawler_name)
            crawler_status = self._get_crawler_status(crawler_name)
            while "READY" not in crawler_status:
                time.sleep(5)
                crawler_status = self._get_crawler_status(crawler_name)




    def _get_all_scripts_locs(self,bucket_name,scripts_folder_loc):
        scripts_locs = []
        scripts_prefix = scripts_folder_loc[scripts_folder_loc.find(bucket_name) + len(bucket_name)+1:] + "/"
        s3_bucket = self.s3_resource.Bucket(bucket_name)
        for obj in s3_bucket.objects.filter(Prefix=scripts_prefix):
            script_loc = obj.key
            if not script_loc.endswith(scripts_prefix):
                scripts_locs.append("s3://{}/{}".format(bucket_name,script_loc))
        return scripts_locs

    def _create_glue_job_from_s3_script(self,job_name,script_loc,extra_py_files_loc,extra_jars_loc):
        job_meta =  self.glue_client.create_job(Name = job_name,Role = "AWSGlueServiceRoleDefault",Command={"Name" : "glueetl", "ScriptLocation" : script_loc, "PythonVersion" : "3"},DefaultArguments={"--extra-py-files" : extra_py_files_loc, "--extra-jars" : extra_jars_loc},MaxRetries=0,MaxCapacity=12.0,Timeout=2880,GlueVersion="3.0")
        
        
    def create_all_glue_jobs_from_s3_scripts(self,bucket_name,scripts_folder_loc,extra_py_files_loc,extra_jars_loc):
        all_scripts_locs = self._get_all_scripts_locs(bucket_name,scripts_folder_loc)
        for script_loc in all_scripts_locs:
            job_name = script_loc[script_loc.rfind("/")+1:].split(".")[0]
            #job_name = job_name+"_v2"
            # add code for checking whether job already exists or not
            all_jobs_meta = self.glue_client.list_jobs(MaxResults=1000)
            if job_name not in all_jobs_meta["JobNames"]:
                self._create_glue_job_from_s3_script(job_name,script_loc,extra_py_files_loc,extra_jars_loc)
	        
	        
    def create_infra(bucket_name,bucket_location,glue_database_name,jdbc_conn_name):
        self.create_bucket(bucket_name,bucket_location)
        self.create_glue_database(glue_database_name)
        self.load_onprem_dependencies(jdbc_conn_name)
