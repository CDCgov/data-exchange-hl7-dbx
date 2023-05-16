# Databricks notebook source
# MAGIC %run ./fn_bronze_table

# COMMAND ----------

eventHubTopic = dbutils.widgets.get("event_hub")

# COMMAND ----------

bronzeDF  = create_structure_validator_df(eventHubTopic, "STRUCTURE-VALIDATOR", globalLakeConfig)
create_bronze_table(eventHubTopic, bronzeDF, globalLakeConfig)
