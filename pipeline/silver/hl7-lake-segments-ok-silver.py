# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook setting 

# COMMAND ----------

TOPIC = "hl7_lake_segments_ok"
STAGE_IN = "bronze"
STAGE_OUT = "silver"

# COMMAND ----------

# MAGIC %run ../common/common_fns

# COMMAND ----------

lake_util = LakeUtil( TableConfig(database_config, TOPIC, STAGE_IN, STAGE_OUT) )


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

# COMMAND ----------

df1 = lake_util.read_stream_from_table()

# not stream for dev only
# df1 = spark.read.format("delta").table( f"{database_config.database}.{TOPIC}_{STAGE_IN}" )
# display( df1 )



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

lake_util.write_stream_to_table(df3)

