# Databricks notebook source
# MAGIC %run ./fn_bronze_table

# COMMAND ----------

TOPIC = "hl7_mmg_validation_ok"
validation_ok  = create_mmg_validator_df(TOPIC, "MMG-VALIDATOR")
create_bronze_table(TOPIC, validation_ok)
