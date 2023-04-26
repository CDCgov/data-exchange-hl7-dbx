# Databricks notebook source
# MAGIC %run ./eh_config

# COMMAND ----------

# MAGIC %run ../common/common_fns

# COMMAND ----------

##### Stream Eventhub data to to Delta Lake
eventHubTopic = "hl7-lake-segments-ok"
transferEventHubDataToLake(globalEventHubConfig, globalLakeConfig, eventHubTopic)

# COMMAND ----------


