# Databricks notebook source
# MAGIC %md
# MAGIC ### Imports 

# COMMAND ----------

# MAGIC %run ./fn_bronze_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Params

# COMMAND ----------

topic = "hl7_mmg_based_ok"

process_name = "mmgBasedTransformer"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform to Bronze

# COMMAND ----------


df1 = createBronzeTable( topic, process_name )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Dev Only

# COMMAND ----------


# display( df1 )

