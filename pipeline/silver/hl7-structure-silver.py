# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook setting 

# COMMAND ----------


TOPIC_ERR = "hl7_structure_err_bronze"
TOPIC_OK = "hl7_structure_ok_bronze"

STAGE_IN = "bronze"
STAGE_OUT = "silver"
TOPIC = "hl7_structure"



# COMMAND ----------

# MAGIC %run ../common/common_fns

# COMMAND ----------


df_err = getTableStream(database_config,TOPIC_ERR)

df_ok = getTableStream(database_config,TOPIC_OK)

lake_util_out = LakeUtil(TableConfig(database_config, TOPIC, STAGE_IN, STAGE_OUT) )

#display(df_err.select("*"))

# check print database_config
#print( lake_util_out.print_database_config() )


# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql.functions import col,concat,explode_outer

df2_ok = df_ok.select("*")
#display(df2_ok)

df2_err = df_err.select("*")

df_result = df_ok.unionByName(df_err, allowMissingColumns=True)

df2 = df_result.select('message_uuid', 'metadata_version','message_info','summary', 'status', 'provenance','process_start_time','report.entries.content','report.entries.structure','report.entries.value-set','error_count','warning_count' )

df3 = df2.withColumn("error_concat",concat(col("content"),col("structure"),col("value-set"))) 
df3 = df3.withColumn('error_concat', explode_outer('error_concat'))

df4 = df3.select('message_uuid','metadata_version',  'message_info', 'summary', 'provenance',  'status','error_concat.line','error_concat.column',df3.error_concat.path.alias("field"),'error_concat.description','error_concat.category')
#display(df4)



# COMMAND ----------

lake_util_out.write_stream_to_table(df4)

