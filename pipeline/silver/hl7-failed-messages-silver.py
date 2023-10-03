# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook setting 

# COMMAND ----------

TOPIC_ERR_MMG_VALIDATION = "hl7_mmg_validation_err_bronze"
TOPIC_ERR_MMG_BASED = "hl7_mmg_based_err_bronze"
TOPIC_ERR_MMG_SQL = "hl7_mmg_sql_err_bronze"
TOPIC_ERR_RECDEB = "hl7_recdeb_err_bronze"
TOPIC_ERR_REDACTED = "hl7_redacted_err_bronze"
TOPIC_ERR_LAKE_SEGMENTS = "hl7_lake_segments_err_bronze"
TOPIC_ERR_STRUCTURE = "hl7_structure_err_bronze"
TOPIC_ERR_JSON_LAKE = "hl7_json_lake_err_bronze"

# COMMAND ----------

# MAGIC %run ../common/common_fns

# COMMAND ----------

# MAGIC %run ../common/schemas

# COMMAND ----------

lakeDAO = LakeDAO(globalLakeConfig)

def get_selected_fields(topic):
    return lakeDAO.readStreamFrom(topic).select('message_uuid', 'message_info', 'provenance', 'processes', 'summary', 'start_processing_time', 'end_processing_time', 'status', 'report')

# COMMAND ----------


df_err_mmg_validation = get_selected_fields(TOPIC_ERR_MMG_VALIDATION)
df_err_mmg_based = get_selected_fields(TOPIC_ERR_MMG_BASED)
df_err_mmg_sql = get_selected_fields(TOPIC_ERR_MMG_SQL)
df_err_recdeb = get_selected_fields(TOPIC_ERR_RECDEB)
df_err_redacted = get_selected_fields(TOPIC_ERR_REDACTED)
df_err_lake_segments = get_selected_fields(TOPIC_ERR_LAKE_SEGMENTS)
df_err_structure = get_selected_fields(TOPIC_ERR_STRUCTURE)
df_err_json_lake = get_selected_fields(TOPIC_ERR_JSON_LAKE)

# COMMAND ----------


from pyspark.sql.functions import col, element_at
from functools import reduce
from pyspark.sql import DataFrame

combined_dfs = [df_err_mmg_validation, df_err_structure, df_err_recdeb, df_err_mmg_based, df_err_mmg_sql, df_err_lake_segments, df_err_redacted]

df_result = reduce(DataFrame.union, combined_dfs)

final_df = df_result.select('message_uuid', 'message_info', 'provenance', 'processes', 'summary.current_status', 'summary.problem.*', 'start_processing_time', 'end_processing_time', 'status', 'report')


#display(final_df)

# COMMAND ----------

lakeDAO.writeStreamTo(final_df, "hl7_failed_messages_silver")
