# Databricks notebook source
# MAGIC %md 
# MAGIC ##### Run All Eventhub Notebooks

# COMMAND ----------

# MAGIC %run  ./eh_hl7_file_dropped_2deltalake

# COMMAND ----------

# MAGIC %run ./eh_hl7_recdeb_err_2deltalake

# COMMAND ----------

# MAGIC %run ./eh_hl7_recdeb_ok_2deltalake

# COMMAND ----------

# MAGIC %run ./eh_hl7_structure_err_2deltalake

# COMMAND ----------

# MAGIC %run ./eh_hl7_structure_ok_2deltalake

# COMMAND ----------

# MAGIC %run ./eh_hl7_mmg_validation_err_2deltalake

# COMMAND ----------

# MAGIC %run ./eh_hl7_mmg_validation_ok_2deltalake

# COMMAND ----------

# MAGIC %run ./eh_hl7_mmg_based_err_2deltalake

# COMMAND ----------

# MAGIC %run ./eh_hl7_mmg_based_ok_2deltalake

# COMMAND ----------

# MAGIC %run ./eh_hl7_mmg_sql_err_2deltalake

# COMMAND ----------

# MAGIC %run ./eh_hl7_mmg_sql_ok_2deltalake

# COMMAND ----------

# MAGIC %run ./eh_hl7_lake_segments_err_2deltalake

# COMMAND ----------

# MAGIC %run ./eh_hl7_lake_segments_ok_2deltalake

# COMMAND ----------

# Eventhub not yet created
#%run ./eh_hl7_quarantine_2deltalake
