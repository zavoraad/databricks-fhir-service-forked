# Databricks notebook source
# MAGIC %pip install git+https://github.com/databrickslabs/dbignite.git

# COMMAND ----------

# DBTITLE 1,Read Sample Data
from  dbignite.fhir_mapping_model import FhirSchemaModel
from pyspark.sql.functions import *
from pyspark.sql.types import * 
import uuid
from dbignite.readers import read_from_directory


#Read in data in a direcotry in r4 for ndjson format
sample_data = "s3://hls-eng-data-public/data/synthea/fhir/fhir/*json"

#Read data from a static directory and parse it using entry() function
bundle = read_from_directory(sample_data, FhirFormat.NDJSON)
df = bundle.entry(schemas =  FhirSchemaModel(schema_version="r4").custom_fhir_resource_mapping(['Patient', 'Claim', 'Condition']))

# COMMAND ----------

df.select(explode("Patient").alias("Patient")).select("Patient.*").show()

# COMMAND ----------

#save this data out to a series of tables
# DBTITLE 1,Build Custom Writer
from multiprocessing.pool import ThreadPool
import multiprocessing as mp
def table_write(entry, column, location = "", write_mode = "overwrite"):
  entry.select(explode(column).alias(column)).select(column + ".*").write.mode(write_mode).saveAsTable( (location + "." + column).lstrip("."))

def bulk_table_write(entry, 
                     location = "",  
                     write_mode = "append", 
                     columns = None):
  pool = ThreadPool(mp.cpu_count()-1)
  list(pool.map(lambda column: table_write(entry, str(column), location, write_mode), ([c for c in entry.columns if c not in ["id", "timestamp", "bundleUUID"]] if columns is None else columns)))

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop schema if exists hls_healthcare.databricks_fhir_service_forked cascade;
# MAGIC create schema hls_healthcare.databricks_fhir_service_forked;

# COMMAND ----------

# DBTITLE 1,Save to Table
df.cache()
bulk_table_write(df, location='hls_healthcare.databricks_fhir_service_forked')
#save all data to tables


# COMMAND ----------

# MAGIC %sql
# MAGIC alter table hls_healthcare.databricks_fhir_service_forked zorder by 
