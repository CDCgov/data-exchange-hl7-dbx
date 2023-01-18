# Databricks notebook source
# MAGIC %md
# MAGIC ### Import Spark SQL Types

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

# MAGIC 
# MAGIC 
# MAGIC %md
# MAGIC ### Schemas Common ( Bronze )

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
# MAGIC ### Schemas Common ( Silver )

# COMMAND ----------

schema_generic_json = MapType(StringType(), StringType())

# {"event_code":"11088","route":"tbrd","mmgs":["mmg:generic_mmg_v2.0","mmg:tbrd"],"reporting_jurisdiction":"51"}
schema_message_info = StructType([    
                       StructField("event_code", StringType(), True),
                       StructField("route", StringType(), True),
                       StructField("mmgs", ArrayType(StringType()), True),
                       StructField("reporting_jurisdiction", StringType(), True),
                     ])


# {"event_id":"f36ea7ab-a01e-0048-6de0-25b10a060d9f","event_timestamp":"2023-01-11T17:16:51.6934553Z","file_uuid":"56c20706-dc2b-4097-9505-d8432b9711b7","file_path":"https://tfedemessagestoragedev.blob.core.windows.net/hl7ingress/TBRD_V1.0.2_TM_TC04.txt","file_timestamp":"2023-01-11T17:16:51+00:00","file_size":7492,"single_or_batch":"SINGLE","message_hash":"dc9b71cb7f1e2544c548cfc529f54233","ext_system_provider":"BLOB","ext_original_file_name":"TBRD_V1.0.2_TM_TC04.txt","message_index":1}
schema_metadata_provenance = StructType([    
                               StructField("event_id", StringType(), True),
                               StructField("event_timestamp", StringType(), True),
                               StructField("file_uuid", ArrayType(StringType()), True),
                               StructField("file_path", StringType(), True),
                               StructField("file_timestamp", StringType(), True),
                               StructField("file_size", StringType(), True),
                               StructField("single_or_batch", StringType(), True),
                               StructField("message_hash", StringType(), True),
                               StructField("ext_system_provider", StringType(), True),
                               StructField("ext_original_file_name", StringType(), True),
                               StructField("message_index", StringType(), True),
                             ])
# mmg-sql-ok tables
schema_tables =  MapType( StringType(), ArrayType(MapType( StringType(), StringType()) ) ) 
