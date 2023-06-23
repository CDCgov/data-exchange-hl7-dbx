# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook setting 

# COMMAND ----------

# MAGIC %run ../common/schemas

# COMMAND ----------

# MAGIC %run ../common/common_fns

# COMMAND ----------

import datetime
from pyspark.sql.functions import *

lakeDAO = LakeDAO(globalLakeConfig)
df1 =  lakeDAO.readStreamFrom("hl7_json_lake_ok_bronze")

 
timestamp = datetime.datetime.now()
json_str = f'''{{"process_name":"hl7_json_lake_ok_silver","created_timestamp":"{timestamp}"}}'''


df1 = df1.withColumn("json_str",lit(json_str))
df1 = df1.withColumn("json_str",from_json("json_str",schema_lake_metadata_processes))
df1 = df1.withColumn("lake_metadata",struct(array_union(col("lake_metadata.processes"),array(col("json_str"))).alias("processes")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Input Table
# MAGIC
# MAGIC # not stream for dev only
# MAGIC # df1 = spark.read.format("delta").table( f"{database_config.database}.{TOPIC}_{STAGE_IN}" )
# MAGIC # display( df1 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select specific columns

# COMMAND ----------

import datetime

df2 = df1.select("message_uuid","message_info","summary","metadata_version","provenance","eventhub_queued_time","eventhub_offset","eventhub_sequence_number","report","lake_metadata")
        


#display( df2 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Output Table

# COMMAND ----------

lakeDAO.writeStreamTo(df2, "hl7_json_lake_ok_silver" )
