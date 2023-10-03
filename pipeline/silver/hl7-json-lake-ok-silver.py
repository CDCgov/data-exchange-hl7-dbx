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

 
df1 = lake_metadata_create(f"hl7_json_lake_ok_silver",df1,"append",globalLakeConfig)

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

df2 = df1.select("message_uuid","message_info","summary","metadata_version","provenance","eventhub_queued_time","eventhub_offset","config","eventhub_sequence_number","report","lake_metadata")
        



#display( df2 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Output Table

# COMMAND ----------

lakeDAO.writeStreamTo(df2, "hl7_json_lake_ok_silver" )
