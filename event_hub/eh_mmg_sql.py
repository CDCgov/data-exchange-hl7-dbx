# Databricks notebook source
# MAGIC %run ./eh_transferData

# COMMAND ----------

transferEventHubDataToLake("hl7_mmg_sql_ok")
