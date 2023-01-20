# Databricks notebook source
# MAGIC %sql
# MAGIC select * from ocio_dex_dev.hl7_recdeb_ok_eh_raw

# COMMAND ----------

 source_db = "ocio_dex_dev"
 target_tbl_name = "hl7_Routes"
 target_schema_name = source_db + "." + target_tbl_name
 chkpoint_loc = "abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/events/" + target_tbl_name + "/_checkpoint"

df1 =  spark.readStream.format("delta").table("ocio_dex_dev.hl7_recdeb_ok_eh_raw")

# COMMAND ----------

from pyspark.sql.functions import *

from pyspark.sql.types import *

schema_evhub_body = StructType([    #StructField("content", StringType(), True), # drop content no longer propagating 
                          StructField("message_info", StringType(), True),
                          StructField("metadata", StringType(), True),
                          StructField("summary", StringType(), True),
                          StructField("message_uuid", StringType(), True),
                          StructField("metadata_version", StringType(), True),
                      ])

schema_messageInfo  = StructType([
                        StructField("event_code", StringType(), True),
                        StructField("route", StringType(), True),
                        StructField("mmgs", ArrayType(StringType(),True), True),
                        StructField("reporting_jurisdiction", StringType(), True),
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

from pyspark.sql import functions as F

df2 = df1.select("body")

df3 = df2.selectExpr("cast(body as string) as json").select( from_json("json", schema_evhub_body).alias("data") ).select("data.*")


df5 = df3.selectExpr("message_uuid", "summary", "metadata_version", "message_info", "cast(metadata as string) as json") \
            .select("message_uuid", "summary", "metadata_version", "message_info",  from_json("json", schema_metadata).alias("data")) \
            .select("message_uuid", "summary", "metadata_version", "message_info", "data.*") \
            .withColumnRenamed("provenance", "metadata_provenance")

'''df5 = df3.selectExpr("message_uuid", "summary", "metadata_version", "cast(message_info as string) as msgjson", "cast(metadata as string) as json") \
            .select("message_uuid", "summary", "metadata_version", from_json("msgjson", schema_messageInfo).alias("msgdata"),  from_json("json", schema_metadata).alias("data")) \
            .select("message_uuid", "summary", "metadata_version", "message_info", "msgdata.*", "data.*") \
            .withColumnRenamed("provenance", "metadata_provenance")'''

df6 = df5.selectExpr("cast(message_info as string) as msgjson") \
          .select(from_json("msgjson",schema_messageInfo).alias("msgdata"))

df7 = df6.select("msgdata.route","msgdata.event_code", "msgdata.mmgs","msgdata.reporting_jurisdiction" )

display(df7)
df7.writeStream.format("delta").outputMode("append").option("checkpointLocation", chkpoint_loc).toTable(target_schema_name)
    

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct route from ocio_dex_dev.hl7_Routes
