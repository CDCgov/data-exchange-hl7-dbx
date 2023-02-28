# Databricks notebook source
# MAGIC %run ./eh_config

# COMMAND ----------

##### Stream Eventhub data to to Delta Lake
eventHubTopic = "hl7-redacted-ok"
transferEventHubDataToLake(eventHubTopic)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ocio_dex_dev.hl7_redacted_ok_eh_raw
