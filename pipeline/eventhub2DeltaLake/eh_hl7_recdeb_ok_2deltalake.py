# Databricks notebook source
import json, os

# COMMAND ----------

# MAGIC %run ./eh_config

# COMMAND ----------

##### Stream Eventhub data to to Delta Lake
eventHubTopic = "hl7-recdeb-ok"
transferEventHubDataToLake(eventHubTopic)

# COMMAND ----------


