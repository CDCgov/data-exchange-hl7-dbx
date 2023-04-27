# Databricks notebook source
# MAGIC %run ./fn_bronze_table

# COMMAND ----------

TOPIC = "hl7_mmg_validation_err"
PROCESS_NAME = "MMG-VALIDATOR"


# COMMAND ----------

validation_err  = create_mmg_validator_df(TOPIC, PROCESS_NAME, globalLakeConfig)
create_bronze_table(TOPIC, validation_err, globalLakeConfig)
