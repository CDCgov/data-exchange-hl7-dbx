# Databricks notebook source
# MAGIC %run ./eh_config

# COMMAND ----------

eventHubTopic = "hl7-lake-segments-err"

##### Stream Eventhub data to to Delta Lake
lakeConfig = LakeConfig(database, database_folder)
ehConfig = EventHubConfig(eventhub_namespace, scope_name, eventHubTopic)

_transferEventHubDataToLake(eventHubTopic, lakeConfig)

# COMMAND ----------


