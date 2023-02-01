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

def createBronzeTable(topic, processName):
  
    lake_util = LakeUtil( TableConfig(database_config, topic, STAGE_IN, STAGE_OUT) )

    rawDF = lake_util.read_stream_from_table()
    
    metadataDF = rawDF.select( from_json("body", schema_evhub_body_v2).alias("data") ).select("data.*")
    
    mdExplodedDF = metadataDF.select("message_uuid", "message_info", "summary", "metadata_version",  \
                                 from_json("metadata.provenance", schema_metadata_provenance).alias("provenance"), from_json("metadata.processes", schema_processes).alias("processes")) 
    
    processExplodedDF = mdExplodedDF.withColumn( "receiver_processes", expr("filter(processes, x -> x.process_name = '" + processName + "')") ) \
           .withColumn( "receiver_process", element_at( col('receiver_processes'), -1) ) \
           .drop( "receiver_processes" ) \
           .select("*", "receiver_process.*") \
           .drop ("receiver_process")
    
    lake_util.write_stream_to_table(processExplodedDF)
    
    return processExplodedDF


# COMMAND ----------

# it can be used in bronze notebook to check, confirm db, table, and checkpoint
def print_bronze_database_config(topic):
    
    lake_util = LakeUtil( TableConfig(database_config, topic, STAGE_IN, STAGE_OUT) )

    # check print database_config
    print( lake_util.print_database_config() )

# COMMAND ----------


