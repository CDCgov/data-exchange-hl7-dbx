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

schema_metadata = StructType([    
    StructField("provenance", StringType(), True),
    StructField("processes", StringType(), True),
 ])

schema_metadata_provenance = StructType([    
    StructField("event_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("file_uuid", StringType(), True),
    StructField("file_path", StringType(), True),
    StructField("file_timestamp", StringType(), True),
    StructField("file_size", StringType(), True),
    StructField("single_or_batch", StringType(), True),
    StructField("message_hash", StringType(), True),
    StructField("ext_system_provider", StringType(), True),
    StructField("ext_original_file_name", StringType(), True),
    StructField("message_index", StringType(), True),
 ])

schema_process = StructType([    
    StructField("status", StringType(), True),
    StructField("process_name", StringType(), True),
    StructField("eventhub_queued_time", StringType(), True),
    StructField("eventhub_offset", IntegerType(), True),
    StructField("eventhub_sequence_number", IntegerType(), True),
    StructField("process_version", StringType(), True),
    StructField("start_processing_time", StringType(), True),
    StructField("end_processing_time", StringType(), True),
    StructField("report", StringType(), True),
   ])

schema_processes = ArrayType(schema_process, True)

schema_message_info = StructType([    
    StructField("event_code", StringType(), True),
    StructField("route", StringType(), True),
    StructField("mmgs", ArrayType(StringType()), True),
    StructField("reporting_jurisdiction", StringType(), True),
    StructField("type", StringType(), True)
])

schema_problem  = StructType ([
    StructField("process_name", StringType(), True),
    StructField("exception_class", StringType(), True),
    StructField("stacktrace", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("should_retry", BooleanType(), True),
    StructField("retry_count", IntegerType(), True),
    StructField("max_retries", IntegerType(), True)
])

schema_summary = StructType ([
    StructField("current_status", StringType(), True),
    StructField("problem", schema_problem, True)
])


schema_reportType = StructType([    
    StructField("line", StringType(), True),
    StructField("column", StringType(), True),
    StructField("path", StringType(), True),
    StructField("description", StringType(), True),
    StructField("category", StringType(), True),
    StructField("classification", StringType(), True),    
 ])
 
schema_entries = StructType([    
    StructField("content", ArrayType(schema_reportType, True)),
    StructField("structure", ArrayType(schema_reportType, True)),
    StructField("value-set", ArrayType(schema_reportType, True)),      
 ])

schema_count = StructType([    
    StructField("structure", IntegerType(), True),
    StructField("content", IntegerType(), True),
    StructField("value-set", IntegerType(), True),      
 ])

schema_report = StructType([    
    StructField("entries", schema_entries, True),
    StructField("error-count", schema_count, True),
    StructField("warning-count", schema_count, True), 
    StructField("status", StringType(), True),
 ])

# COMMAND ----------

issueTypeSchema = StructType([StructField("classification", StringType(), True),
                      StructField("category", StringType(), True),
                      StructField("fieldName", StringType(), True),
                      StructField("path", StringType(), True),
                      StructField("line", StringType(), True),
                      StructField("errorMessage", StringType(), True),
                      StructField("description", StringType(), True) ])

mmgReportSchema = StructType([StructField("entries", ArrayType(issueTypeSchema, True), True),
                              StructField("error-count", IntegerType(), True),
                              StructField("warning-count",  IntegerType(), True),
                              StructField("status", StringType(), True)])


    
schema_validation_bronze = StructType([
     StructField("message_uuid", StringType(), False),
     StructField("message_info", schema_message_info, True),
     StructField("provenance",   schema_metadata_provenance, True),
     StructField("processes",    schema_processes, True),
     StructField("process_name", StringType(), True),
     StructField("process_version", StringType(), True),
     StructField("start_processing_time", StringType(), True),
     StructField("end_processing_time", StringType(), True),
    # Validation Specific (all the above is common to all bronze tables)
     StructField("error-count", IntegerType(), True),
     StructField("warning-count", IntegerType(), True),
     StructField("status", StringType(), True),
     StructField("report", StringType(), True)
])

schema_validation_silver = StructType([
    StructField("message_uuid", StringType(), True),
    StructField("message_info", schema_message_info, True),
    StructField("category", StringType(), True),
    StructField("line", IntegerType(), True),
    StructField("column", IntegerType(), True),
    StructField("field", StringType(), True),
    StructField("description", StringType(), True)
])

# COMMAND ----------

## New Version...
schema_evhub_body_v2 = StructType([    #StructField("content", StringType(), True), # drop content no longer propagating 
    StructField("message_info", schema_message_info, True),
    StructField("summary", schema_summary, True),
    StructField("message_uuid", StringType(), True),
    StructField("metadata_version", StringType(), True),
    StructField("metadata", schema_metadata, True)
])



# COMMAND ----------

# MAGIC %md
# MAGIC ### Schemas Common ( Silver )

# COMMAND ----------

schema_generic_json = MapType(StringType(), StringType())

# mmg-sql-ok tables
schema_tables =  MapType( StringType(), ArrayType(MapType( StringType(), StringType()) ) ) 

# lake segments
schema_segment = StructType([    
    StructField("segment", StringType(), True),
    StructField("segment_number", IntegerType(), True),
    StructField("parent_segments", ArrayType(StringType()), True),
])

schema_lake_segments = ArrayType(schema_segment, True)

#Redactor
schema_Redactor = StructType([    
    StructField("path", StringType(), True),
    StructField("rule", StringType(), True),
    StructField("lineNumber", IntegerType(), True),
])

schema_Redactor_Report = StructType([StructField("entries",ArrayType(schema_Redactor, True),True),
                                      StructField("status", StringType(), True)])
