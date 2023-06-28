# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook setting 

# COMMAND ----------

# MAGIC %run ../common/common_fns

# COMMAND ----------

# MAGIC %run ../common/schemas

# COMMAND ----------

lakeDAO = LakeDAO(globalLakeConfig)

df1 =  lakeDAO.readStreamFrom("hl7_mmg_sql_ok_bronze")


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
        .withColumnRenamed("report", "mmg_sql_model_string")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations

# COMMAND ----------

df3 = df2.withColumn("mmg_sql_model_map", from_json(col("mmg_sql_model_string"), schema_generic_json)) \
         .drop("mmg_sql_model_string")


# COMMAND ----------

df4 = df3.withColumn( "mmg_sql_model_singles",  map_filter("mmg_sql_model_map", lambda k, _: k != "tables" ) ) \
         .withColumn( "mmg_sql_model_tables", from_json( col("mmg_sql_model_map.tables" ), schema_tables) ) \
         .drop( "mmg_sql_model_map" )     
df4 = lake_metadata_create("hl7_mmg_sql_ok_silver",df4,"append")
#display(df4.select("lake_metadata.processes").where("lake_metadata.processes is not null"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Output Table

# COMMAND ----------

lakeDAO.writeStreamTo(df4, "hl7_mmg_sql_ok_silver" )
