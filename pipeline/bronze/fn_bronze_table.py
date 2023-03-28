# Databricks notebook source
# MAGIC %md
# MAGIC ### Imports 

# COMMAND ----------

# MAGIC %run ../common/setup_env

# COMMAND ----------

# MAGIC %run ../common/schemas

# COMMAND ----------

from pyspark.sql.functions import *

def create_structure_validator_df(topic, process_name, lake_config):
    standard_df = create_bronze_df(topic, process_name, lake_config)
    structure_validator_df = standard_df.withColumn("struct_report", from_json(col("report"), schema_report)).withColumn("error_count", col("struct_report.error-count.structure") + col("struct_report.error-count.value-set") + col("struct_report.error-count.content" )) \
   .withColumn("warning_count", col("struct_report.warning-count.structure") + col("struct_report.warning-count.value-set") + \
              col("struct_report.warning-count.content" )).drop("struct_report")
    
    return structure_validator_df

def create_mmg_validator_df(topic, process_name, lake_config):
    standard_df = create_bronze_df(topic, process_name, lake_config)
    mmg_validator_df = standard_df.withColumn("struct_report", from_json(col("report"), mmgReportSchema)).withColumn("error_count", col("struct_report.error-count")) \
   .withColumn("warning_count", col("struct_report.warning-count")).drop("struct_report")
    
    return mmg_validator_df

def create_bronze_df(topic, process_name, lake_config):
    lakeDAO = LakeDAO(lake_config)
    rawDF = lakeDAO.readStreamFrom(f"{topic}_eh_raw")
    
    metadataDF = rawDF.select( from_json("body", schema_evhub_body_v2).alias("data") ).select("data.*")
    
    mdExplodedDF = metadataDF.select("message_uuid", "message_info", "summary", "metadata_version",  \
        from_json("metadata.provenance", schema_metadata_provenance).alias("provenance"), from_json("metadata.processes", schema_processes).alias("processes"))   

    processExplodedDF = mdExplodedDF.withColumn("receiver_processes", expr(f"filter(processes, x -> x.process_name = '{process_name}')")) \
               .withColumn( "receiver_process", element_at( col('receiver_processes'), -1) ) \
               .drop( "receiver_processes" ) \
               .select("*", "receiver_process.*") \
               .drop ("receiver_process")

    return processExplodedDF

def create_bronze_table(topic, input_df, lakeConfig):
    lakeDAO = LakeDAO(lakeConfig)
    lakeDAO.writeStreamTo(input_df, f"{topic}_bronze")


