# Databricks notebook source
# MAGIC %md
# MAGIC ### Imports 

# COMMAND ----------

# MAGIC %run ../common/common_fns

# COMMAND ----------

# MAGIC %run ../common/schemas

# COMMAND ----------

#TODO: change to streaming
# df1 = spark.readStream.format("delta").table( input_table )
from pyspark.sql.functions import *

def createBronzeTable(topic, processName):
    ## Prepare params
    db_name  = "ocio_dex_dev"
    root_folder = 'abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/' 
    lakeConfig = LakeConfig(root_folder, db_name)

    input_table = topic + "_eh_raw"
    output_table = topic + "_bronze"
    ## Read Raw info
    rawDF = spark.readStream.format("delta").option("ignoreDeletes", "true").table( lakeConfig.getSchemaName(input_table) )
    metadataDF = rawDF.select( from_json("body", schema_evhub_body_v2).alias("data") ).select("data.*")
    
    mdExplodedDF = metadataDF.select("message_uuid", "message_info", "summary", "metadata_version",  \
                                 from_json("metadata.provenance", schema_metadata_provenance).alias("provenance"), from_json("metadata.processes", schema_processes).alias("processes")) 
    
    processExplodedDF = mdExplodedDF.withColumn( "receiver_processes", expr("filter(processes, x -> x.process_name = '" + processName + "')") ) \
           .withColumn( "receiver_process", element_at( col('receiver_processes'), -1) ) \
           .drop( "receiver_processes" ) \
           .select("*", "receiver_process.*") \
           .drop ("receiver_process")
    
    processExplodedDF.writeStream.format("delta").outputMode("append").option("checkpointLocation", lakeConfig.getCheckpointLocation(output_table)).toTable(lakeConfig.getSchemaName(output_table))
    
#     df.writeStream(...._)
    return processExplodedDF


# COMMAND ----------

# recdebOk = createBronzeTable("hl7_mmg_based_ok", "mmgBasedTransformer")

# display(recdebOk)
