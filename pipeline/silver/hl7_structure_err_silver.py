# Databricks notebook source
# MAGIC %sql
# MAGIC  SELECT * FROM ocio_dex_dev.hl7_structure_err_bronze 
# MAGIC  

# COMMAND ----------

 source_db = "ocio_dex_dev"
 target_tbl_name = "hl7_structure_err_silver"
 target_schema_name = source_db + "." + target_tbl_name
 chkpoint_loc = "abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/events/" + target_tbl_name + "/_checkpoint"


df =  spark.readStream.format("delta").option("ignoreDeletes", "true").table("ocio_dex_dev.hl7_structure_err_bronze")

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.functions import col,concat

df2 = df.select('message_uuid', 'metadata_version','message_info','summary',  'provenance','report.entries.content','report.entries.structure','report.entries.value-set','error_count' )

#concat 3 arrays(structure,content, valueset)
df3 = df2.withColumn("error_concat",concat(col("content"),col("structure"),col("value-set")))


df3 = df3.withColumn('error_concat', F.explode('error_concat'))


df4 = df3.select('message_uuid','metadata_version',  'message_info', 'summary', 'provenance', 'error_concat.line','error_concat.column',df3.error_concat.path.alias("field"),'error_concat.description','error_concat.category')
display(df4)



# COMMAND ----------

df4.writeStream.format("delta").outputMode("append").option("checkpointLocation", chkpoint_loc).toTable(target_schema_name)




# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ocio_dex_dev.hl7_structure_err_silver 

# COMMAND ----------


