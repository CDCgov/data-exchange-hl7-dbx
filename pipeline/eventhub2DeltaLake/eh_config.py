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


# COMMAND ----------

# DBTITLE 1,Opinionated method that knows the Event Hub, and Lake configurations. All it needs is what Topic to load!
# def transferEventHubDataToLake(eventHubTopic, lakeConfig, eventHubConfig):
#     ehConfig = EventHubConfig(ev_namespace, scope_name, eventHubTopic)
    
#     _transferEventHubDataToLake(ehConfig, lakeConfig)

# COMMAND ----------

## Pull data from Eventhub to Delta Lake
#eventHubTopic = "hl7-file-dropped"
#transferEventHubDataToLake(eventHubTopic)

# COMMAND ----------

# MAGIC %sql 
# MAGIC --Drop table ocio_ede_dev.hl7_file_dropped_eh_raw_test

# COMMAND ----------


