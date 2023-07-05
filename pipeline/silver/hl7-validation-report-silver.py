# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook setting 

# COMMAND ----------

# MAGIC %run ../common/common_fns

# COMMAND ----------

# MAGIC %run ../common/schemas

# COMMAND ----------


lakeDAO = LakeDAO(globalLakeConfig)

df_structure_err = lakeDAO.readStreamFrom("hl7_structure_err_bronze")
df_structure_ok =  lakeDAO.readStreamFrom("hl7_structure_ok_bronze")

df_structure_elr_ok =  lakeDAO.readStreamFrom("hl7_structure_elr_ok_bronze")

df_mmg_err = lakeDAO.readStreamFrom("hl7_mmg_validation_err_bronze")
df_mmg_ok =  lakeDAO.readStreamFrom("hl7_mmg_validation_ok_bronze")


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, concat, explode_outer, from_json

### Structure validation related logic
df_structure_result = df_structure_ok.unionByName(df_structure_err, allowMissingColumns=True).unionByName(df_structure_elr_ok, allowMissingColumns=True)

df2_structure = df_structure_result.withColumn("report", from_json('report', schema_report)).select('message_uuid', 'metadata_version','message_info','summary', 'status', 'provenance','start_processing_time','config','report.entries.content','report.entries.structure','report.entries.value-set','error_count','warning_count', 'process_name' )

df3_structure = df2_structure.withColumn("error_concat",concat(col("content"),col("structure"),col("value-set"))) 
df3_structure = df3_structure.withColumn('error_concat', explode_outer('error_concat'))

df4_structure = df3_structure.select('message_uuid','metadata_version',  'message_info', 'summary', 'provenance',  'status','config','error_concat.line','error_concat.column','error_concat.path','error_concat.fieldName','error_concat.description','error_concat.classification','error_concat.category', 'process_name')

### MMG validation related logic
df_mmg_result = df_mmg_ok.unionByName(df_mmg_err, allowMissingColumns=True)

df2_mmg = df_mmg_result.withColumn('issue', F.explode_outer((from_json('report', mmgReportSchema)).entries))

df3_mmg = df2_mmg.select('message_uuid', 'metadata_version','message_info','summary', 'provenance','status', 'config','issue.line', 'issue.path','issue.fieldName',
                 'issue.description','issue.classification', 'issue.category', 'process_name' )



### Combine both Structure and MMG validation dataframes
df_combined_result = df3_mmg.unionByName(df4_structure, allowMissingColumns=True)

#display(df_combined_result)

# COMMAND ----------

lakeDAO.writeStreamTo(df_combined_result, "hl7_validation_report_silver" )
