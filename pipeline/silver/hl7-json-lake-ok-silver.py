# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook setting 

# COMMAND ----------

# MAGIC %run ../common/common_fns

# COMMAND ----------

lakeDAO = LakeDAO(globalLakeConfig)
df1 =  lakeDAO.readStreamFrom("hl7_json_lake_ok_bronze")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports 

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schemas Needed

# COMMAND ----------

# MAGIC %run ../common/schemas

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

df2 = df1.select("message_uuid","message_info","summary","metadata_version","provenance","eventhub_queued_time","eventhub_offset","config","eventhub_sequence_number","report")
        

#display( df2 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Output Table

# COMMAND ----------

lakeDAO.writeStreamTo(df2, "hl7_json_lake_ok_silver" )
