from utils import *

# Define data sources and parameters for ingestion
bucket_name = "c24-data-extract"
file_path = "additional_datasources/assistance_db/Lookup_Calendar.csv"
output_table_name = "Lookup_Calendar"

# Defining the table ddl for preserving data types
table_ddl = """
    CREATE TABLE {} (
       `Date - Date Format` DATE,
       `Date - Text Format` DATE,
        Day STRING,
        `Week Ending` DATE,	
        `Calendar Week No` INT,
        `Calendar Month` STRING,
        Month INT,
        Year INT,
        `Financial Period` FLOAT,
        `Forecast Period` DOUBLE
    )
""".format(output_table_name)

utils = Utils()

# Persisting the raw data table to datalake
utils.ingest_csv_datalake(bucket_name,file_path,output_table_name,table_ddl)