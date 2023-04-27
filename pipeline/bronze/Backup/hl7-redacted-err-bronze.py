# Databricks notebook source
# MAGIC %md
# MAGIC ### Imports 

# COMMAND ----------

# MAGIC %run ./fn_bronze_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Params

# COMMAND ----------

TOPIC = "hl7_redacted_err"

PROCESS_NAME = "REDACTOR"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform to Bronze

# COMMAND ----------

redacted_err = create_bronze_df( TOPIC, PROCESS_NAME )
create_bronze_table(TOPIC, redacted_err)
