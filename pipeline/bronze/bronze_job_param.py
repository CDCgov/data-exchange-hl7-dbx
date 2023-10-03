# Databricks notebook source
# MAGIC %run ./fn_bronze_table

# COMMAND ----------

processNames = {
    'hl7-file-dropped-ok': 'FILE_DROPPED',
    'hl7-recdeb-ok' : 'RECEIVER',
    'hl7-recdeb-err': 'RECEIVER',
    'hl7-redacted-ok': 'REDACTOR',
    'hl7-redacted-err': 'REDACTOR',
    # 'hl7-structure-ok': 'STRUCTURE-VALIDATOR',
    # 'hl7-structure-elr-ok': 'STRUCTURE-VALIDATOR',
    # 'hl7-structure-err': 'STRUCTURE-VALIDATOR',
    # 'hl7-mmg-validation-ok': 'MMG-VALIDATOR',
    # 'hl7-mmg-validation-err': 'MMG-VALIDATOR',
    'hl7-mmg-based-ok': 'MMG_BASED_TRANSFORMER',
    'hl7-mmg-based-err': 'MMG_BASED_TRANSFORMER',
    'hl7-mmg-sql-ok': 'MMG_BASED_SQL_TRANSFORMER',
    'hl7-mmg-sql-err': 'MMG_BASED_SQL_TRANSFORMER',
    'hl7-lake-segments-ok': 'LAKE-SEGMENTS-TRANSFORMER',
    'hl7-lake-segments-err': 'LAKE-SEGMENTS-TRANSFORMER',
    'hl7-json-lake-ok':'HL7-JSON-LAKE-TRANSFORMER',
    'hl7-json-lake-err':'HL7-JSON-LAKE-TRANSFORMER'
}

# COMMAND ----------

eventHubTopic = dbutils.widgets.get("event_hub")
processName = processNames[eventHubTopic]



# COMMAND ----------

bronzeDF  = create_bronze_df(eventHubTopic, processName, globalLakeConfig)
#display(bronzeDF.select("lake_metadata").where("lake_metadata.processes is not null"))
create_bronze_table(eventHubTopic, bronzeDF, globalLakeConfig)
