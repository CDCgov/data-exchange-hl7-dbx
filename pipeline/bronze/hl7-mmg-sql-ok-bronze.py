# Databricks notebook source
from pyspark.sql.functions import *

from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Input and Output Tables

# COMMAND ----------

inputTable = "ocio_dex_dev.hl7_mmg_sql_ok_eh_raw"
outputTable = "ocio_dex_dev.hl7_mmg_sql_ok_bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schemas Needed

# COMMAND ----------

schema_evhub_body = StructType([    #StructField("content", StringType(), True), # drop content no longer propagating 
                          StructField("message_info", StringType(), True),
                          StructField("metadata", StringType(), True),
                          StructField("summary", StringType(), True),
                          StructField("message_uuid", StringType(), True),
                          StructField("metadata_version", StringType(), True),
                      ])

schema_metadata = StructType([    
                       StructField("provenance", StringType(), True),
                       StructField("processes", StringType(), True),
                     ])

schema_process = StructType([    
                       StructField("status", StringType(), True),
                       StructField("process_name", StringType(), True),
                       StructField("process_version", StringType(), True),
                       StructField("start_processing_time", StringType(), True),
                       StructField("end_processing_time", StringType(), True),
                       StructField("report", StringType(), True),
                     ])

schema_processes = ArrayType(schema_process, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Input

# COMMAND ----------

df1 = spark.read.format("delta").table( inputTable )

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
# MAGIC ### Write Output

# COMMAND ----------

#option("mergeSchema", "true") - dev only
    
df8.write.mode('overwrite').saveAsTable( outputTable )
