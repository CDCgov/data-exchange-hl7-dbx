# Databricks notebook source
# MAGIC %md
# MAGIC ### Imports 

# COMMAND ----------

# MAGIC %run ./fn_bronze_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Params

# COMMAND ----------

TOPIC = "hl7_redacted_ok"

PROCESS_NAME = "REDACTOR"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform to Bronze

# COMMAND ----------

redacted_ok = create_bronze_df( TOPIC, PROCESS_NAME, globalLakeConfig )
create_bronze_table(TOPIC, redacted_ok, globalLakeConfig)
