# Databricks notebook source
# MAGIC %run ./fn_bronze_table

# COMMAND ----------

processNames = {
    'hl7-file-dropped-ok': 'FILE_DROPPED',
    'hl7-recdeb-ok' : 'RECEIVER',
    'hl7-recdeb-err': 'RECEIVER',
    'hl7-structure-ok': 'STRUCTURE-VALIDATOR',
    'hl7-structure-elr-ok': 'STRUCTURE-VALIDATOR',
    'hl-structure-err': 'STRUCTURE-VALIDATOR',
    'hl7-mmg-validation-ok': 'MMG-VALIDATOR',
    'hl7-mmg-validation-err': 'MMG-VALIDATOR',
    'hl7-lake-segments-ok': 'lakeSegsTransformer',
    'hl7-lake-segments-err': 'lakeSegsTransformer'
}

# COMMAND ----------

eventHubTopic = dbutils.widgets.get("event_hub")
processName = processNames[eventHubTopic]



# COMMAND ----------

bronzeDF  = create_bronze_df(eventHubTopic, processName, globalLakeConfig)
create_bronze_table(eventHubTopic, bronzeDF, globalLakeConfig)
