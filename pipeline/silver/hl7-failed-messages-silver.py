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

STAGE_IN = "bronze"
STAGE_OUT = "silver"
TOPIC = "hl7_failed_messages"



# COMMAND ----------

# MAGIC %run ../common/common_fns

# COMMAND ----------

# MAGIC %run ../common/schemas

# COMMAND ----------

def get_selected_fields(topic):
    return getTableStream(database_config, topic).select('message_uuid', 'message_info', 'provenance', 'processes', 'summary')

# COMMAND ----------

df_err_mmg_validation = get_selected_fields(TOPIC_ERR_MMG_VALIDATION)
df_err_mmg_based = get_selected_fields(TOPIC_ERR_MMG_BASED)
df_err_mmg_sql = get_selected_fields(TOPIC_ERR_MMG_SQL)
df_err_recdeb = get_selected_fields(TOPIC_ERR_RECDEB)
df_err_redacted = get_selected_fields(TOPIC_ERR_REDACTED)
df_err_lake_segments = get_selected_fields(TOPIC_ERR_LAKE_SEGMENTS)
df_err_structure = get_selected_fields(TOPIC_ERR_STRUCTURE)

lake_util_out = LakeUtil( TableConfig(database_config, TOPIC, STAGE_IN, STAGE_OUT) )

# COMMAND ----------


from pyspark.sql.functions import col, element_at
from functools import reduce
from pyspark.sql import DataFrame

combined_dfs = [df_err_mmg_validation, df_err_structure, df_err_recdeb, df_err_mmg_based, df_err_mmg_sql, df_err_lake_segments, df_err_redacted]
df_result = reduce(DataFrame.union, combined_dfs)

df1 = df_result.select('message_uuid', 'message_info', 'provenance', 'processes', 'summary.current_status', 'summary.problem.*')

processExplodedDF = df1.withColumn( "last_process", element_at( col('processes'), -1) ) \
               .select("*", "last_process.start_processing_time", "last_process.end_processing_time", "last_process.status", "last_process.report") \
               .drop ("last_process")


#display(processExplodedDF)

# COMMAND ----------

lake_util_out.write_stream_to_table(processExplodedDF)
