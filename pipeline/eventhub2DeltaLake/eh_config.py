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
    df = spark.readStream.format("eventhubs").options(**ehConfig).option("maxEventsPerTrigger","10000000").load()
    df = df.withColumn("body", df["body"].cast("string"))
   
    # Standardize on Table names for Event Hub topics:
    tbl_name = normalizeString(topic) + "_eh_raw"

    df = lake_metadata_create(tbl_name,df,"insert",lakeConfig)


    lakeDAO = LakeDAO(lakeConfig)
    lakeDAO.writeStreamTo(df, tbl_name)

