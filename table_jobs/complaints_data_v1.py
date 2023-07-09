from utils import *

# Define data sources and parameters for ingestion
bucket_name = "c24-data-extract"
file_path = "additional_datasources/Complaints_Data_V1/Complaints Data V1.csv"
output_table_name = "complaints_data_v1"

utils = Utils()

# Persisting the raw data table to datalake
utils.ingest_csv_datalake(bucket_name,file_path,output_table_name)