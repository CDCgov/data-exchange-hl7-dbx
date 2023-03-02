# Databricks notebook source
# DBTITLE 1,Import Functions
# MAGIC %run ./fn_bronze_table

# COMMAND ----------

df1 = createBronzeTable("hl7_redacted_ok", "REDACTOR")
