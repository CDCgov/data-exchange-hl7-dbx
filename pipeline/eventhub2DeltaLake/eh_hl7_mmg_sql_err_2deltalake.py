# Databricks notebook source
import json, os

# COMMAND ----------

# MAGIC %run ./eh_config

# COMMAND ----------

##### Stream Eventhub data to to Delta Lake
eventHubTopic = "hl7-mmg-sql-err"
transferEventHubDataToLake(eventHubTopic)

# COMMAND ----------


