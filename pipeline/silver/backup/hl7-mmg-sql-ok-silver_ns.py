# Databricks notebook source
# MAGIC %md
# MAGIC ### Imports 

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Input and Output Tables

# COMMAND ----------

input_table = "ocio_dex_dev.hl7_mmg_sql_ok_bronze"
output_table = "ocio_dex_dev.hl7_mmg_sql_ok_silver"

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
# MAGIC ### Drop and Rename Columns

# COMMAND ----------

df2 = df1.drop("processes", "status", "process_name", "process_version", "start_processing_time", "end_processing_time") \
        .withColumnRenamed("report", "mmg_sql_model_string")

display( df2 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations

# COMMAND ----------

df3 = df2.withColumn("mmg_sql_model_map", from_json(col("mmg_sql_model_string"), schema_generic_json)) \
         .drop("mmg_sql_model_string")

display( df3 )

# COMMAND ----------

df4 = df3.withColumn( "mmg_sql_model_singles",  map_filter("mmg_sql_model_map", lambda k, _: k != "tables" ) ) \
         .withColumn( "mmg_sql_model_tables", from_json( col("mmg_sql_model_map.tables" ), schema_tables) ) \
         .drop( "mmg_sql_model_map" )

display( df4 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Output Table

# COMMAND ----------


#TODO: change to streaming and append, with checkpoint output_checkpoint

df4.write.mode('overwrite').saveAsTable( output_table )
