# Databricks notebook source
# MAGIC %md
# MAGIC ### Imports 

# COMMAND ----------

# MAGIC %run ./fn_bronze_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Params

# COMMAND ----------

TOPIC = "hl7_mmg_based_err"

PROCESS_NAME = "mmgBasedTransformer"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform to Bronze

# COMMAND ----------


mmg_based_err = create_bronze_df( TOPIC, PROCESS_NAME )
create_bronze_table(TOPIC, mmg_based_err)

