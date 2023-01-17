# Databricks notebook source
# MAGIC %sql
# MAGIC  SELECT * FROM ocio_dex_dev.hl7_structure_err_bronze

# COMMAND ----------

 source_db = "ocio_dex_dev"
 target_tbl_name = "hl7_structure_err_silver"
 target_schema_name = source_db + "." + target_tbl_name
 chkpoint_loc = "abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/events/" + target_tbl_name + "/_checkpoint5"


df =  spark.readStream.format("delta").table("ocio_dex_dev.hl7_structure_err_bronze")

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
from datetime import datetime

from pyspark.sql import functions as F

df2 = df.select('message_uuid', 'message_info.route', 'message_hash', 'provenance.message_index','process_start_time', 'process_end_time', 'provenance.ext_original_file_name', 'report.entries.content','errorCount' )


df3 = df2.withColumn('content', F.explode('content'))

df4 = df3.select('message_uuid','route', 'message_hash', 'message_index', 'process_start_time', 'process_end_time','ext_original_file_name', 'content.line','content.column','content.path','content.description','content.category','content.stacktrace')

display(df4)


# COMMAND ----------

df4.writeStream.format("delta").outputMode("append").option("checkpointLocation", chkpoint_loc).toTable(target_schema_name)




# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ocio_dex_dev.hl7_structure_err_silver
