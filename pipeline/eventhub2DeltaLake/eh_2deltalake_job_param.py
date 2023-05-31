# Databricks notebook source
# MAGIC %run ../common/common_fns

# COMMAND ----------

# MAGIC %run ./eh_config

# COMMAND ----------

##### Stream Eventhub data to to Delta Lake
import os


eventHubTopic = dbutils.widgets.get("event_hub")
transferEventHubDataToLake(globalEventHubConfig, globalLakeConfig, eventHubTopic)
