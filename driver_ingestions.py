import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)


# Declaring ETL pipeline parameters

bucket_name = "c24-data-extract"
bucket_region = "eu-west-2"
onprem_database_name = "col_anon"
glue_database_name = "crisis-24"
jdbc_conn_name = "crisis-24-onprem-nis-conn"

metadata_loc = "s3://{}/metadata/dependencies_metadata.csv".format(bucket_name)
scripts_folder_loc = "s3://{}/scripts/jobs".format(bucket_name)
extra_py_files_loc = "s3://{}/scripts/utils.py".format(bucket_name)
extra_jars_loc = "s3://{}/jars/delta-core_2.12-1.0.0.jar".format(bucket_name)

max_batch_size = 25


# Importing automation and helper scripts

from infra_utils import *
from dependency_utils import *
from utils import *
from reporting_utils import *


# Creating infrastructure from code 

infra_utils = SetUpInfraUtils()
infra_utils.create_infra(bucket_name,bucket_location,glue_database_name,jdbc_conn_name)

# Creation of glue jobs from s3 based scripts
infra_utils.create_all_glue_jobs_from_s3_scripts(bucket_name,scripts_folder_loc,extra_py_files_loc,extra_jars_loc)

# Defining ETL dependencies (layers)

dependency_utils = DependencyUtils()
layered_jobs = dependency_utils.get_layered_jobs(metadata_loc)


# Executing ingestion jobs according to dependencies

run_utils = RunUtils()
base_utils = BaseUtils()

for layer in layered_jobs:
    batchs = base_utils.generate_batchs(layer,max_batch_size)
    for batch in batchs:
	    run_utils.run_parallel(batch)

# Creating ingestion report

utils = Utils()
reporting_utils = ReportingUtils(utils)

reporting_utils.generate_recon_report(bucket_name,onprem_database_name)



job.commit()