# Databricks notebook source
# MAGIC %run ../common/setup_env

# COMMAND ----------

# MAGIC %run ../common/schemas

# COMMAND ----------

# MAGIC %run ../common/common_fns

# COMMAND ----------

import datetime

# COMMAND ----------

def transferEventHubDataToLake(eventHubConfig, lakeConfig, topic):
    ehConfig = eventHubConfig.getConfig(topic)
    eventhub_max_events_per_trigger = os.getenv("eventhub_max_events_per_trigger") or "100000000"
    df = spark.readStream.format("eventhubs").options(**ehConfig).option("maxEventsPerTrigger",eventhub_max_events_per_trigger).load()
    df = df.withColumn("body", df["body"].cast("string"))
   
    # Standardize on Table names for Event Hub topics:
    tbl_name = normalizeString(topic) + "_eh_raw"

    df = lake_metadata_create(tbl_name,df,"insert",lakeConfig)


    lakeDAO = LakeDAO(lakeConfig)
    lakeDAO.writeStreamTo(df, tbl_name)

