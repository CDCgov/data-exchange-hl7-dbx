# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook setting 

# COMMAND ----------

# MAGIC %run ../common/common_fns

# COMMAND ----------

# MAGIC %run ../common/schemas

# COMMAND ----------


lakeDAO = LakeDAO(globalLakeConfig)

df_err    = lakeDAO.readStreamFrom("hl7_structure_err_bronze")
df_ok     = lakeDAO.readStreamFrom("hl7_structure_ok_bronze")
df_elr_ok = lakeDAO.readStreamFrom("hl7_structure_elr_ok_bronze")

#display(df_err.select("*"))

# check print database_config
#print( lake_util_out.print_database_config() )


# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql.functions import col, concat, explode_outer, from_json

# df2_ok = df_ok.select("*")
#display(df2_ok)

# df2_err = df_err.select("*")

df_result = df_ok.unionByName(df_err, allowMissingColumns=True).unionByName(df_elr_ok, allowMissingColumns=True)

df2 = df_result.withColumn("report", from_json('report', schema_report)).select('message_uuid', 'metadata_version','message_info','summary', 'status', 'provenance','start_processing_time','report.entries.content','report.entries.structure','report.entries.value-set','error_count','warning_count' )

df3 = df2.withColumn("error_concat",concat(col("content"),col("structure"),col("value-set"))) 
df3 = df3.withColumn('error_concat', explode_outer('error_concat'))

df4 = df3.select('message_uuid','metadata_version',  'message_info', 'summary', 'provenance',  'status','error_concat.line','error_concat.column',df3.error_concat.path.alias("field"),'error_concat.description','error_concat.classification','error_concat.category')
#display(df4)

# COMMAND ----------

# lake_util_out.write_stream_to_table(df4)

lakeDAO.writeStreamTo(df4, "hl7_structure_silver" )
