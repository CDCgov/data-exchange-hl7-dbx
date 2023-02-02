# Databricks notebook source
# MAGIC %run  ./hl7-structure-err-bronze

# COMMAND ----------

# MAGIC %run  ./hl7-structure-ok-bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ocio_dex_dev.hl7_structure_err_eh_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ocio_dex_dev.hl7_structure_err_bronze
