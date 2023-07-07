# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook setting 

# COMMAND ----------

# MAGIC %run ../common/common_fns

# COMMAND ----------

# MAGIC %run ../common/schemas

# COMMAND ----------

lakeDAO = LakeDAO(globalLakeConfig)

df1 =  lakeDAO.readStreamFrom("hl7_mmg_based_ok_bronze")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports 

# COMMAND ----------

from pyspark.sql.functions import *

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
df3 = lake_metadata_create("hl7_mmg_based_ok_silver",df3,"append")
#display( df3.select("lake_metadata.processes").where("lake_metadata.processes is not null") )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Output Table

# COMMAND ----------

lakeDAO.writeStreamTo(df3, "hl7_mmg_based_ok_silver" )

