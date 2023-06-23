# Databricks notebook source
# MAGIC %run ../common/setup_env

# COMMAND ----------

# MAGIC %run ../common/schemas

# COMMAND ----------

import datetime

# COMMAND ----------

def transferEventHubDataToLake(eventHubConfig, lakeConfig, topic):
    ehConfig = eventHubConfig.getConfig(topic)

    




    df = spark.readStream.format("eventhubs").options(**ehConfig).load()
    
    
    

    # Standardize on Table names for Event Hub topics:
    tbl_name = normalizeString(topic) + "_eh_raw"

    timestamp = datetime.datetime.now()
    json_string = f'''{{"processes":[{{"process_name":"{tbl_name}","created_timestamp":"{timestamp}"}}]}}'''
    df = df.withColumn("body", df["body"].cast("string"))
    
    df = df.withColumn("lake_metadata",lit(json_string))
    df = df.withColumn("lake_metadata",from_json("lake_metadata",schema_lake_metadata))

    lakeDAO = LakeDAO(lakeConfig)
    lakeDAO.writeStreamTo(df, tbl_name)

