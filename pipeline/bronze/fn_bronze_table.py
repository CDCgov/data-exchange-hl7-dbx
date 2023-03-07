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

def create_structure_validator_df(topic, process_name):
    standard_df = create_bronze_df(topic, process_name)
    structure_validator_df = standard_df.withColumn("report", from_json(col("report"), schema_report)).withColumn("error_count", col("report.error-count.structure") + col("report.error-count.value-set") + col("report.error-count.content" )) \
   .withColumn("warning_count", col("report.warning-count.structure") + col("report.warning-count.value-set") + \
              col("report.warning-count.content" ))
    
    return structure_validator_df

def create_mmg_validator_df(topic, process_name):
    standard_df = create_bronze_df(topic, process_name)
    mmg_validator_df = standard_df.withColumn("report", from_json(col("report"), mmgReportSchema)).withColumn("error_count", col("report.error-count")) \
   .withColumn("warning_count", col("report.warning-count"))
    
    return mmg_validator_df

def create_bronze_df(topic, process_name):
    lake_util = LakeUtil( TableConfig(database_config, topic, STAGE_IN, STAGE_OUT) )

    rawDF = lake_util.read_stream_from_table()
    
    metadataDF = rawDF.select( from_json("body", schema_evhub_body_v2).alias("data") ).select("data.*")
    
    mdExplodedDF = metadataDF.select("message_uuid", "message_info", "summary", "metadata_version",  \
        from_json("metadata.provenance", schema_metadata_provenance).alias("provenance"), from_json("metadata.processes", schema_processes).alias("processes"))   

    processExplodedDF = mdExplodedDF.withColumn("receiver_processes", expr(f"filter(processes, x -> x.process_name = '{process_name}')")) \
               .withColumn( "receiver_process", element_at( col('receiver_processes'), -1) ) \
               .drop( "receiver_processes" ) \
               .select("*", "receiver_process.*") \
               .drop ("receiver_process")

    return processExplodedDF

def create_bronze_table(topic, input_df):
    lake_util = LakeUtil( TableConfig(database_config, topic, STAGE_IN, STAGE_OUT) )
    lake_util.write_stream_to_table(input_df)

