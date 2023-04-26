# Databricks notebook source
# MAGIC %run ./fn_bronze_table

# COMMAND ----------

processNames = {
    'hl7-file-dropped-ok': 'FILE_DROPPED',
    'hl7-recdeb-ok' : 'RECEIVER',
    'hl7-recdeb-err': 'RECEIVER',
    'hl7-structure-ok': 'STRUCTURE-VALIDATOR',
    'hl7-structure-elr-ok': 'STRUCTURE-VALIDATOR',
    'hl7-structure-err': 'STRUCTURE-VALIDATOR',
    'hl7-mmg-validation-ok': 'MMG-VALIDATOR',
    'hl7-mmg-validation-err': 'MMG-VALIDATOR',
    'hl7-mmg-based-ok': 'mmgBasedTransformer',
    'hl7-mmg-based-ERR': 'mmgBasedTransformer',
    'hl7-mmg-sql-ok': 'MMG-SQL-TRANSFORMER',
    'hl7-mmg-sql-err': 'MMG-SQL-TRANSFORMER',
    'hl7-lake-segments-ok': 'LAKE-SEGMENTS-TRANSFORMER',
    'hl7-lake-segments-err': 'LAKE-SEGMENTS-TRANSFORMER'
}

# COMMAND ----------

eventHubTopic = dbutils.widgets.get("event_hub")
processName = processNames[eventHubTopic]



# COMMAND ----------

bronzeDF  = create_bronze_df(eventHubTopic, processName, globalLakeConfig)
create_bronze_table(eventHubTopic, bronzeDF, globalLakeConfig)
