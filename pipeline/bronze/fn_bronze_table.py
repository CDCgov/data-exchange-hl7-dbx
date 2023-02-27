# Databricks notebook source
# MAGIC %md
# MAGIC ### Imports 

# COMMAND ----------

# MAGIC %run ../common/common_fns

# COMMAND ----------

# MAGIC %run ../common/schemas

# COMMAND ----------

STAGE_IN = "eh_raw"
STAGE_OUT = "bronze"

# COMMAND ----------

from pyspark.sql.functions import *
        
def createBronzeStructureValidator(topic, processName):
    lake_util = LakeUtil( TableConfig(database_config, topic, STAGE_IN, STAGE_OUT) )
    rawDF = lake_util.read_stream_from_table()    
    df1 = rawDF.withColumn("bodyJson", from_json(col("body"), s_schema))
    df2 = df1.select("bodyJson.*")
    df3 = df2.withColumn("processes", col("metadata.processes"))
    df4 = df3.withColumn("structureReport", explode("processes") ).filter( f"structureReport.process_name = '{processName}'").select("message_uuid", "message_info", "summary", "metadata_version", "metadata.provenance", "metadata.processes", "structureReport") \
  .withColumn("report", col("structureReport.report")) \
  .withColumn("process_name", col("structureReport.process_name")) \
  .withColumn("process_version", col("structureReport.process_version")) \
  .withColumn("status", col("structureReport.status")) \
  .withColumn("start_processing_time", col("structureReport.start_processing_time")) \
  .withColumn("end_processing_time", col("structureReport.end_processing_time")) \
  .withColumn("error_count", col("report.error-count.structure") + col("report.error-count.value-set") + col("report.error-count.content" )) \
  .withColumn("warning_count", col("report.warning-count.structure") + col("report.warning-count.value-set") + \
              col("report.warning-count.content" ))
    df5 = df4.drop("structureReport")
    lake_util.write_stream_to_table(df5)
    return df5

def createBronzeMMGValidator(topic, processName):
    lake_util = LakeUtil( TableConfig(database_config, topic, STAGE_IN, STAGE_OUT) )
    rawDF = lake_util.read_stream_from_table()    
    metadataDF = rawDF.select( from_json("body", schema_evhub_body_v2).alias("data") ).select("data.*")
    mdExplodedDF = metadataDF.select("message_uuid", "message_info", "summary", "metadata_version",  \
       from_json("metadata.provenance", schema_metadata_provenance).alias("provenance"), from_json("metadata.processes", \
       schema_processes).alias("processes")) 
    df1 = mdExplodedDF.withColumn("processReport", explode("processes") ).filter( f"processReport.process_name = '{processName}'")
    df_mmgreport = df1.withColumn("report", from_json(col("processReport.report"), mmgReportSchema))
    df_final = df_mmgreport.select("message_uuid", "message_info", "summary", "metadata_version", "provenance", "processes", \
                    "processReport.process_name",  "processReport.process_version", "processReport.start_processing_time", \
                    "processReport.end_processing_time", "report") \
                    .withColumn("error_count", col("report.error-count")) \
                    .withColumn("warning_count", col("report.warning-count")) \
                    .withColumn("status", col("report.status"))

    lake_util.write_stream_to_table(df_final)
    return df_final

def createBronzeTable(topic, processName):
    lake_util = LakeUtil( TableConfig(database_config, topic, STAGE_IN, STAGE_OUT) )

    rawDF = lake_util.read_stream_from_table()
    
    metadataDF = rawDF.select( from_json("body", schema_evhub_body_v2).alias("data") ).select("data.*")
    
    mdExplodedDF = metadataDF.select("message_uuid", "message_info", "summary", "metadata_version",  \
        from_json("metadata.provenance", schema_metadata_provenance).alias("provenance"), from_json("metadata.processes", schema_processes).alias("processes"))   

    processExplodedDF = mdExplodedDF.withColumn("receiver_processes", expr(f"filter(processes, x -> x.process_name = '{processName}')")) \
               .withColumn( "receiver_process", element_at( col('receiver_processes'), -1) ) \
               .drop( "receiver_processes" ) \
               .select("*", "receiver_process.*") \
               .drop ("receiver_process")

    lake_util.write_stream_to_table(processExplodedDF)
    return processExplodedDF

