# Databricks notebook source
# MAGIC %run ./fn_bronze_table

# COMMAND ----------

validation_ok  = createBronzeMMGValidator("hl7_mmg_validation_ok", "MMG-VALIDATOR")
