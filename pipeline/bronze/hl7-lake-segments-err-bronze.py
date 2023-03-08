# Databricks notebook source
# MAGIC %md
# MAGIC ### Imports 

# COMMAND ----------

# MAGIC %run ./fn_bronze_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Params

# COMMAND ----------

TOPIC = "hl7_lake_segments_err"

PROCESS_NAME = "lakeSegsTransformer"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform to Bronze

# COMMAND ----------


segments_err = create_bronze_df( TOPIC, PROCESS_NAME )
create_bronze_table(TOPIC, segments_err)

# COMMAND ----------


