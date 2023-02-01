# Databricks notebook source
# DBTITLE 1,Import Functions
# MAGIC %run ./fn_bronze_table

# COMMAND ----------

structureOk  = createBronzeStructureValidator("hl7_structure_ok", "STRUCTURE-VALIDATOR")

# COMMAND ----------

# print_bronze_database_config("hl7_structure_ok")

# COMMAND ----------

# %sql SELECT * FROM ocio_dex_dev.hl7_structure_ok_bronze

# COMMAND ----------

# chkpoint_loc = "abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/events/hl7_structure_ok_bronze/_checkpoint"
# structureOk.writeStream.format("delta").outputMode("append").option("checkpointLocation", chkpoint_loc).toTable("ocio_dex_dev.hl7_structure_ok_bronze")
