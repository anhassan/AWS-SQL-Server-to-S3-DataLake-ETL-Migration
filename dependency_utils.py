import sys
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from awsglue.job import Job
import time

spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context.getOrCreate())
spark = glue_context.spark_session
job = Job(glue_context)

class DependencyUtils(object):
    
    def __init__(self):
        self.glue_client = boto3.client("glue")
    
    def get_layered_jobs(self,metadata_loc):
        layered_jobs = []

        dependencies_csv = spark.read.option("header","True").csv(metadata_loc)

        all_jobs_meta = self.glue_client.list_jobs(MaxResults=1000)
        all_jobs = all_jobs_meta["JobNames"]if "JobNames" in all_jobs_meta.keys() else []
        all_nis_jobs = [job.replace("nis_", "") for job in all_jobs if job.startswith("nis")]

        dependencies = [[row["Table"], row["Parent Table"], row["Layer"],  "nis_{}".format(row["Table"].lower()) ]for row in dependencies_csv.collect() if row["Table"].lower() in all_nis_jobs]
        max_layer = int(max([dependency[2] for dependency in dependencies]))

        for num_layer in range(0,max_layer+1):
            layered_jobs.append([dependency[-1] for dependency in dependencies if dependency[2] == str(num_layer)])
        
        return layered_jobs
    