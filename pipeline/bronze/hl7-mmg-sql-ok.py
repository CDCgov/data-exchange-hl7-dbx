# Databricks notebook source
from pyspark.sql.functions import *

from pyspark.sql.types import *

# COMMAND ----------

df1 = spark.read.format("delta").table( "ocio_dex_dev.hl7_mmg_sql_ok_eh_raw" )

display( df1 )

# COMMAND ----------

df2 = df1.select("body")

display( df2 )

# COMMAND ----------

schema1 = StructType([    #StructField("content", StringType(), True), # drop content no longer propagating 
                          StructField("message_info", StringType(), True),
                          StructField("metadata", StringType(), True),
                          StructField("summary", StringType(), True),
                          StructField("message_uuid", StringType(), True),
                          StructField("metadata_version", StringType(), True),
                      ])

# COMMAND ----------

df3 = df2.selectExpr("cast(body as string) as json").select(from_json("json", schema1).alias("data")).select("data.*")

display( df3 )

# COMMAND ----------

schema2 = StructType([    
                       StructField("provenance", StringType(), True),
                       StructField("processes", StringType(), True),
                     ])

# COMMAND ----------

df4 = df3.selectExpr("message_uuid", "summary", "metadata_version", "message_info", "cast(metadata as string) as json") \
            .select("message_uuid", "summary", "metadata_version", "message_info",  from_json("json", schema2).alias("data")) \
            .select("message_uuid", "summary", "metadata_version", "message_info", "data.*") \
            .withColumnRenamed("provenance", "metadata_provenance")
            

display( df4 )

# COMMAND ----------

schema4 = StructType([    
                       StructField("status", StringType(), True),
                       StructField("process_name", StringType(), True),
                       StructField("process_version", StringType(), True),
                       StructField("start_processing_time", StringType(), True),
                       StructField("end_processing_time", StringType(), True),
                       StructField("report", StringType(), True),
                     ])


schema3 = ArrayType(schema4, True)


# COMMAND ----------

df5 = df4.selectExpr("message_uuid", "summary", "metadata_version", "metadata_provenance", "message_info", "cast(processes as string)") \
         .select("message_uuid", "summary", "metadata_version", "metadata_provenance", "message_info", from_json("processes", schema3).alias("processes"))

display( df5 )

# COMMAND ----------

df6 = df5.withColumn("mmg_sql_model_arr", expr("filter(processes, x -> x.process_name = 'mmgSQLTransformer')")) \
         .withColumn( "mmg_sql_model", element_at( col('mmg_sql_model_arr'), -1)["report"] ) \
        .drop("processes", "mmg_sql_model_arr")
                     
display( df6 )

# COMMAND ----------

df7 = df6.filter(df6.message_uuid.isNotNull())

display( df7 )

# COMMAND ----------

df7.write.mode('overwrite').saveAsTable( "ocio_dex_dev.hl7_mmg_sql_ok_bronze" )

# COMMAND ----------


