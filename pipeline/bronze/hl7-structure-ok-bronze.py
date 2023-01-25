# Databricks notebook source
# DBTITLE 1,Import Functions
# MAGIC %run ./fn_bronze_table

# COMMAND ----------

structureOk  = createBronzeTable("hl7_structure_ok", "STRUCTURE-VALIDATOR")
