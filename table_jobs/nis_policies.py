
from utils import *

# Define the data sources and destinations
database = "col_anon"
dest_datalake = "data-extract-crisis"
dest_table = "nis_policies"


utils = Utils()

# Reading data tables from on premise databases
nis_policies = utils.read_from_onprem(database,"nis.policies")
nis_organisations = utils.read_from_onprem(database,"nis.organisations")

# Creating temp views from data tables
nis_policies.createOrReplaceTempView("nis_policies")
nis_organisations.createOrReplaceTempView("nis_organisations")

# Defining filtering query
filter_query = """
    SELECT pol.*
    FROM nis_policies as pol
    INNER JOIN nis_organisations as o1  on o1.id = pol.AgentId
    INNER JOIN nis_organisations as o2  on o2.id = o1.ParentId
    WHERE o2.id IN
    (40,100,187,189,190,192,194,197,199,201,212,214,217,219,
    221,222,236,237,240,242,245,407,612,620,702,712,838,855,
    884,889,953,967,974,979,986,989,995,1008,1014,1019,1047,
    1056,1076,1091,1098,1116,1133,1146,1185,1197,1217,1222,
    1230,1240,1291,1355,1383,1399,1451,1560,1653)
"""

# Filtering the data table using fitering rule
filtered_nis_policies = spark.sql(filter_query)

# Persisting the filtered data table to datalake
utils.write_to_datalake(filtered_nis_policies,dest_datalake,dest_table)

# Dropping temp views of data tables
spark.catalog.dropTempView("nis_policies")
spark.catalog.dropTempView("nis_organisations")