# Databricks notebook source
# DBTITLE 1,Load Helper Notebook
# MAGIC %run ./eh_transferData

# COMMAND ----------

transferEventHubDataToLake("hl7-file-dropped")

# COMMAND ----------

# DBTITLE 1,Configure Event Hub
# ev_namespace    = "tf-eventhub-namespace-dev"
# ev_topic        = "hl7-file-dropped"
# ev_sas_key_name = os.getenv("v_hl7_file_dropped_key")
# ev_sas_key_val  = os.getenv("v_hl7_file_dropped_key_val")

# ehConfig = EventHubConfig(ev_namespace, ev_topic, ev_sas_key_name, ev_sas_key_val)

# COMMAND ----------

# DBTITLE 1,Configure Lake
# db_name ="ocio_ede_dev"
# tbl_name = "tbl_hl7_file_dropped"
# root_folder = "/tmp/delta/"

# lakeConfig = LakeConfig(root_folder, db_name, tbl_name)

# COMMAND ----------

# DBTITLE 1,Execute Transfer Data
# transferEventHubDataToLake(ehConfig, lakeConfig)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Count(*) FROM ocio_ede_dev.tbl_hl7_file_dropped;

# COMMAND ----------

# MAGIC %sql 
# MAGIC --Drop table ocio_ede_dev.tbl_hl7_file_dropped;
