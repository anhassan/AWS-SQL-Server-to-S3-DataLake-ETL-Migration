from utils import *

# Define data sources and parameters for ingestion
bucket_name = "c24-data-extract"
file_path = "additional_datasources/360_View/360 View.csv"
output_table_name = "360_view"

utils = Utils()

# Persisting the raw data table to datalake
utils.ingest_csv_datalake(bucket_name,file_path,output_table_name)