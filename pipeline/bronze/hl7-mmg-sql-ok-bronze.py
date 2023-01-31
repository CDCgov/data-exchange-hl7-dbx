# Databricks notebook source
# MAGIC %md
# MAGIC ### Imports 

# COMMAND ----------

# MAGIC %run ./fn_bronze_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Params

# COMMAND ----------

topic = "hl7_mmg_sql_ok"

process_name = "mmgSQLTransformer"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform to Bronze

# COMMAND ----------

# it can be used in bronze notebook to check, confirm db, table, and checkpoint:
# print_bronze_database_config(topic)


df1 = createBronzeTable( topic, process_name )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dev Only

# COMMAND ----------


# display( df1 )

