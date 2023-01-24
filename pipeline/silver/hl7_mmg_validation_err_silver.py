# Databricks notebook source
# MAGIC %md
# MAGIC Updated - 1/23/23
# MAGIC BY: Ramanbir (swy4)

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT * FROM ocio_dex_dev.hl7_mmg_validation_err_bronze 

# COMMAND ----------

source_db = "ocio_dex_dev"
target_tbl_name = "hl7_mmg_validation_err_silver"
target_schema_name = source_db + "." + target_tbl_name
chkpoint_loc = "abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/events/" + target_tbl_name + "/_checkpoint"

df =  spark.readStream.format("delta").option("ignoreDeletes", "true").table("ocio_dex_dev.hl7_mmg_validation_err_bronze")
display(df)

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.functions import col,concat

df2 = df.withColumn('issue', F.explode('report.entries'))

df3 = df2.select('message_uuid', 'metadata_version','message_info','summary', 'provenance', 'issue.category','issue.line', 
                 df2.issue.path.alias("column"),df2.issue.fieldName.alias("field"),'issue.description' )

#df4 = df3.select('message_uuid','metadata_version',  'message_info', 'summary', 'provenance', #'error_concat.line','error_concat.column',df3.error_concat.path.alias("field"),'error_concat.description','error_concat.category')

display(df3)

# COMMAND ----------

print(target_schema_name)
df3.writeStream.format("delta").outputMode("append").option("checkpointLocation", chkpoint_loc).toTable(target_schema_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP table ocio_dex_dev.hl7_mmg_validation_err_silver 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ocio_dex_dev.hl7_mmg_validation_err_silver 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC ocio_dex_dev.hl7_mmg_validation_err_silver 

# COMMAND ----------


