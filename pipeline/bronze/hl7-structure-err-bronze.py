# Databricks notebook source
# DBTITLE 1,Import Functions
# MAGIC %run ./fn_bronze_table

# COMMAND ----------

# print_bronze_database_config("hl7_structure_err")

# COMMAND ----------

structureErr  = createBronzeStructureValidator("hl7_structure_err", "STRUCTURE-VALIDATOR")

# COMMAND ----------

# %sql SELECT * from ocio_dex_dev.hl7_structure_err_bronze
