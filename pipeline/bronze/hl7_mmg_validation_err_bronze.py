# Databricks notebook source
# MAGIC %run ./fn_bronze_table

# COMMAND ----------

TOPIC = "hl7_mmg_validation_err"
validation_err  = create_mmg_validator_df(TOPIC, "MMG-VALIDATOR")
create_bronze_table(TOPIC, validation_err)
