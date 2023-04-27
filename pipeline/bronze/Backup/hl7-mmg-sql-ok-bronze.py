# Databricks notebook source
# MAGIC %md
# MAGIC ### Imports 

# COMMAND ----------

# MAGIC %run ./fn_bronze_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Params

# COMMAND ----------

TOPIC = "hl7_mmg_sql_ok"

PROCESS_NAME = "mmgSQLTransformer"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform to Bronze

# COMMAND ----------

mmg_sql_ok = create_bronze_df( TOPIC, PROCESS_NAME )
create_bronze_table(TOPIC, mmg_sql_ok)
