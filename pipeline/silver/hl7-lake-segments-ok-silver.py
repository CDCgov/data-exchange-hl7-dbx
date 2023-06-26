# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook setting 

# COMMAND ----------

# MAGIC %run ../common/common_fns

# COMMAND ----------

lakeDAO = LakeDAO(globalLakeConfig)
df1 =  lakeDAO.readStreamFrom("hl7_lake_segments_ok_bronze")


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
# MAGIC ### Drop and Rename Columns

# COMMAND ----------

df2 = df1.drop("summary", "metadata_version", "processes", "status", "process_name", "process_version", "start_processing_time", "end_processing_time") \
        .withColumnRenamed("report", "lake_segments_string")

# display( df2 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations

# COMMAND ----------

df3 = df2.withColumn( "lake_segments_arr", from_json( col("lake_segments_string"), schema_lake_segments) ) \
         .drop("lake_segments_string") \
         .withColumn("segment_struct", explode(col('lake_segments_arr'))) \
         .drop("lake_segments_arr") \
         .withColumn("segment_number", col('segment_struct.segment_number')) \
         .withColumn("segment", col('segment_struct.segment')) \
         .withColumn("parent_segments", col('segment_struct.parent_segments')) \
         .drop("segment_struct")

# display( df3 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Output Table

# COMMAND ----------

lakeDAO.writeStreamTo(df3, "hl7_lake_segments_ok_silver" )
