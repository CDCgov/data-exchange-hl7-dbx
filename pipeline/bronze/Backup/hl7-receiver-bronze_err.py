# Databricks notebook source
# DBTITLE 1,Import Functions
# MAGIC %run ./fn_bronze_table

# COMMAND ----------

TOPIC = "hl7_recdeb_err"
PROCESS_NAME = "RECEIVER"

# COMMAND ----------

receiver_err = create_bronze_df( TOPIC, PROCESS_NAME )
create_bronze_table(TOPIC, receiver_err)
