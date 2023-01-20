# Databricks notebook source
# DBTITLE 1,Import Functions
# MAGIC %run ./fn_bronze_table

# COMMAND ----------

receiverOK  = createBronzeTable("hl7_recdeb_ok", "RECEIVER")
