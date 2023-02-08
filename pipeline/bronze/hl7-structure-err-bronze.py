# Databricks notebook source
# DBTITLE 1,Import Functions
# MAGIC %run ./fn_bronze_table

# COMMAND ----------

structureErr  = createBronzeStructureValidator("hl7_structure_err", "STRUCTURE-VALIDATOR")
