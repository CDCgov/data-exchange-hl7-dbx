# Databricks notebook source
# MAGIC %run ./fn_bronze_table

# COMMAND ----------

validation_ok  = createBronzeTable("hl7_mmg_validation_err", "MMG-VALIDATOR")
