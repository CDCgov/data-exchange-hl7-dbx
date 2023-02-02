# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook setting 

# COMMAND ----------


TOPIC_ERR = "hl7_structure_err_bronze"
STAGE_IN = "bronze"
STAGE_OUT = "silver"

TOPIC_OK = "hl7_structure_ok_bronze"
TOPIC = "hl7_structure"



# COMMAND ----------

# MAGIC %run ../common/common_fns

# COMMAND ----------


#lake_util_err = LakeUtil(TableConfig(database_config,TOPIC_ERR,STAGE_IN, STAGE_OUT))
struct_err_stream = getTableStream(database_config,TOPIC_ERR)
df_err = struct_err_stream.getReadStream()


struct_ok_stream = getTableStream(database_config,TOPIC_OK)
df_ok = struct_ok_stream.getReadStream()


lake_util_out = LakeUtil( TableConfig(database_config, TOPIC, STAGE_IN, STAGE_OUT) )
#display(df_er.getReadStream().select("*"))
# check print database_config
print( lake_util_out.print_database_config() )


# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql.functions import col,concat

df2_ok = df_ok.select("*")
#display(df2_ok)
df2_err = df_err.select("*")
#display(df2_err)

df_result = df_ok.unionByName(df_err, allowMissingColumns=True)
#display(df_result)

df2 = df_result.select('message_uuid', 'metadata_version','message_info','summary', 'status', 'provenance','report.entries.content','report.entries.structure','report.entries.value-set','error_count' )

#concat 3 arrays(structure,content, valueset)
df3 = df2.withColumn("error_concat",concat(col("content"),col("structure"),col("value-set")))


df3 = df3.withColumn('error_concat', F.explode('error_concat'))


df4 = df3.select('message_uuid','metadata_version',  'message_info', 'summary', 'provenance', 'status','error_concat.line','error_concat.column',df3.error_concat.path.alias("field"),'error_concat.description','error_concat.category')
display(df4)

# COMMAND ----------

lake_util.write_stream_to_table(df4)
#df4.writeStream.format("delta").outputMode("append").trigger(availableNow=True).option("checkpointLocation", chkpoint_loc).toTable(target_schema_name)
 #df.writeStream.format("delta").outputMode("append").trigger(availableNow=True).option("checkpointLocation", self.table_config.output_checkpoint() ).toTable( self.table_config.output_database_table()
