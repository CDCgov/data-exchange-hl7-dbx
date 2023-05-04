# Databricks notebook source
# MAGIC %run ../common/setup_env

# COMMAND ----------

def transferEventHubDataToLake(eventHubConfig, lakeConfig, topic):
    ehConfig = eventHubConfig.getConfig(topic)
    df = spark.readStream.format("eventhubs").options(**ehConfig).load()
    df = df.withColumn("body", df["body"].cast("string"))
    
    # Standardize on Table names for Event Hub topics:
    tbl_name = normalizeString(topic) + "_eh_raw"
    
    lakeDAO = LakeDAO(lakeConfig)
    lakeDAO.writeStreamTo(df, tbl_name)

