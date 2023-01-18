# Databricks notebook source
# MAGIC %md
# MAGIC ### Imports 

# COMMAND ----------

from pyspark.sql.functions import *

from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Input and Output Tables

# COMMAND ----------

# TODO: change to Lake config for DB name and checkpoint name

input_table = "ocio_dex_dev.hl7_mmg_sql_ok_eh_raw"
output_table = "ocio_dex_dev.hl7_mmg_sql_ok_bronze"

output_checkpoint = "abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/events/" + output_table + "/_checkpoint"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schemas Needed

# COMMAND ----------

# MAGIC %run ../common/schemas

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Input Table

# COMMAND ----------

#TODO: change to streaming
# df1 = spark.readStream.format("delta").table( input_table )


df1 = spark.read.format("delta").table( input_table )

display( df1 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations

# COMMAND ----------

df2 = df1.select("body")

display( df2 )

# COMMAND ----------

df3 = df2.selectExpr("cast(body as string) as json").select( from_json("json", schema_evhub_body).alias("data") ).select("data.*")

display( df3 )

# COMMAND ----------

df4 = df3.filter( df3.message_uuid.isNotNull() )

display( df4 )

# COMMAND ----------

df5 = df4.selectExpr("message_uuid", "summary", "metadata_version", "message_info", "cast(metadata as string) as json") \
            .select("message_uuid", "summary", "metadata_version", "message_info",  from_json("json", schema_metadata).alias("data")) \
            .select("message_uuid", "summary", "metadata_version", "message_info", "data.*") \
            .withColumnRenamed("provenance", "metadata_provenance")
            

display( df5 )

# COMMAND ----------

df6 = df5.selectExpr("message_uuid", "summary", "metadata_version", "metadata_provenance", "message_info", "cast(processes as string)") \
         .select("message_uuid", "summary", "metadata_version", "metadata_provenance", "message_info", from_json("processes", schema_processes).alias("processes"))

display( df6 )

# COMMAND ----------

df7 = df6.withColumn( "mmg_sql_model_arr", expr("filter(processes, x -> x.process_name = 'mmgSQLTransformer')") ) \
           .withColumn( "process_mmg_sql_model", element_at( col('mmg_sql_model_arr'), -1) ) \
           .drop( "mmg_sql_model_arr" )
                     
display( df7 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Make Final Output DataFrame

# COMMAND ----------

df8 = df7.withColumn( "process_status", col("process_mmg_sql_model.status") ) \
        .withColumn( "process_name", col("process_mmg_sql_model.process_name") ) \
        .withColumn( "process_version", col("process_mmg_sql_model.process_version") ) \
        .withColumn( "start_processing_time", col("process_mmg_sql_model.start_processing_time") ) \
        .withColumn( "end_processing_time", col("process_mmg_sql_model.end_processing_time") ) \
        .withColumn( "process_report", col("process_mmg_sql_model.report") ) \
        .drop( "process_mmg_sql_model" )


display( df8 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Output Table

# COMMAND ----------

#option("mergeSchema", "true") - dev only

#TODO: change to streaming and append, with checkpoint output_checkpoint
df8.write.mode('overwrite').saveAsTable( output_table )
