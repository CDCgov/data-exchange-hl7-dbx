# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook setting 

# COMMAND ----------

TOPIC = "hl7_mmg_validation_ok"
STAGE_IN = "bronze"
STAGE_OUT = "silver"

# COMMAND ----------

# MAGIC %run ../common/common_fns

# COMMAND ----------

lake_util = LakeUtil( TableConfig(database_config, TOPIC, STAGE_IN, STAGE_OUT) )

# check print database_config
print( lake_util.print_database_config() )

# COMMAND ----------

df = lake_util.read_stream_from_table()

# COMMAND ----------

# spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.functions import col,concat

df2 = df.withColumn('issue', F.explode('report.entries'))

df3 = df2.select('message_uuid', 'metadata_version','message_info','summary', 'provenance', 'issue.category','issue.line', 
                 df2.issue.path.alias("column"),df2.issue.fieldName.alias("field"),'issue.description' )

#df4 = df3.select('message_uuid','metadata_version',  'message_info', 'summary', 'provenance', #'error_concat.line','error_concat.column',df3.error_concat.path.alias("field"),'error_concat.description','error_concat.category')

# display(df3)

# COMMAND ----------

lake_util.write_stream_to_table(df3)
