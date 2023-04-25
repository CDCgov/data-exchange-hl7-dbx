# Databricks notebook source
# MAGIC %run ../common/common_fns

# COMMAND ----------

# MAGIC %run ./eh_config

# COMMAND ----------

##### Stream Eventhub data to to Delta Lake
eventHubTopic = "hl7-redacted-ok"
transferEventHubDataToLake(globalEventHubConfig, globalLakeConfig, eventHubTopic)
