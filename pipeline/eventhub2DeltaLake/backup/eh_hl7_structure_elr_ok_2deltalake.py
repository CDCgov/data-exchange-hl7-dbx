# Databricks notebook source
# MAGIC %run ./eh_config

# COMMAND ----------

##### Stream Eventhub data to to Delta Lake
eventHubTopic = "hl7-structure-elr-ok"
transferEventHubDataToLake(eventHubTopic)
