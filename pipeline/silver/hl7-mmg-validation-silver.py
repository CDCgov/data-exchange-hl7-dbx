# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook setting 

# COMMAND ----------


TOPIC_ERR = "hl7_mmg_validation_err_bronze"
TOPIC_OK = "hl7_mmg_validation_ok_bronze"

STAGE_IN = "bronze"
STAGE_OUT = "silver"
TOPIC = "hl7_mmg_validation"



# COMMAND ----------

# MAGIC %run ../common/common_fns

# COMMAND ----------

df_err = getTableStream(database_config,TOPIC_ERR)

df_ok = getTableStream(database_config,TOPIC_OK)


lake_util_out = LakeUtil( TableConfig(database_config, TOPIC, STAGE_IN, STAGE_OUT) )
#display(df_er.getReadStream().select("*"))
# check print database_config
#print( lake_util_out.print_database_config() )


# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql.functions import col,concat

df2_ok = df_ok.select("*")

df2_err = df_err.select("*")

df_result = df_ok.unionByName(df_err, allowMissingColumns=True)

df2 = df_result.withColumn('issue', F.explode('report.entries'))

df3 = df2.select('message_uuid', 'metadata_version','message_info','status','summary', 'provenance', 'issue.category','issue.line', 
                 df2.issue.path.alias("column"),df2.issue.fieldName.alias("field"),'issue.classification','issue.description' )


#display(df3)

# COMMAND ----------

lake_util_out.write_stream_to_table(df3)
