# Databricks notebook source
# MAGIC %md
# MAGIC ### Imports 

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Input and Output Tables

# COMMAND ----------

input_db = "ocio_dex_dev"
input_table = "hl7_mmg_based_ok_bronze"
output_db = "ocio_dex_dev"
output_table = "hl7_mmg_based_ok_silver"

source = f"{input_db}.{input_table}"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schemas Needed

# COMMAND ----------

# MAGIC %run ../common/schemas

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Input Table

# COMMAND ----------


df1 = spark.readStream.format("delta").option("ignoreDeletes", "true").table( source )

#display( df1 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop and Rename Columns

# COMMAND ----------

df2 = df1.drop("processes", "status", "process_name", "process_version", "start_processing_time", "end_processing_time") \
        .withColumnRenamed("report", "mmg_based_model_string")

# display( df2 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations

# COMMAND ----------

df3 = df2.withColumn( "mmg_based_model_map", from_json( col("mmg_based_model_string"), schema_generic_json) ) \
         .drop("mmg_based_model_string")

# display( df3 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Output Table

# COMMAND ----------

target_schema_name = f"{output_db}.{output_table}"
chkpoint_loc = f"abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/events/{output_table}/_checkpoint"

df3.writeStream.format("delta").outputMode("append").trigger(availableNow=True).option("checkpointLocation", chkpoint_loc).toTable(target_schema_name)
