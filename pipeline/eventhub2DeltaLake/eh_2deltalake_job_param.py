# Databricks notebook source
# MAGIC %run ../common/common_fns

# COMMAND ----------

# MAGIC %run ./eh_config

# COMMAND ----------

# MAGIC %run ../common/auditLog

# COMMAND ----------

##### Stream Eventhub data to to Delta Lake
import os
import datetime


eventHubTopic = dbutils.widgets.get("event_hub")
ct = str(datetime.datetime.now())

df = [(ct,"Info",eventHubTopic,"eh_2deltalake intiated")]
create_audit_log_table(df)
try:
    transferEventHubDataToLake(globalEventHubConfig, globalLakeConfig, eventHubTopic)
except Exception as e:
    df = [(ct,"error",eventHubTopic,str(e))]
    create_audit_log_table(df)
