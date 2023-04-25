# Databricks notebook source
# DBTITLE 1,Import Functions
# MAGIC %run ./fn_bronze_table

# COMMAND ----------

TOPIC = "hl7_structure_err"
PROCESS_NAME = "STRUCTURE-VALIDATOR"

# COMMAND ----------

structure_err  = create_structure_validator_df(TOPIC, PROCESS_NAME)
create_bronze_table(TOPIC, structure_err)
