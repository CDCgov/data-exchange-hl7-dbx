# Databricks notebook source
import json, os
ev_namespace    = "tf-eventhub-namespace-dev"
ev_sas_key_name = os.getenv("v_tf_eventhub_namespace_dev_key")
ev_sas_key_val  = os.getenv("v_tf_eventhub_namespace_dev_key_val")
ev_name         = "hl7-file-dropped"

conn_string="Endpoint=sb://{0}.servicebus.windows.net/;EntityPath={1};SharedAccessKeyName={2};SharedAccessKey={3}".format(ev_namespace, ev_name, ev_sas_key_name, ev_sas_key_val)

ehConf = {}
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(conn_string)
##print(conn_string)

# COMMAND ----------

##### Creating an Event Hubs Source for Streaming Queries
df = spark.readStream.format("eventhubs").options(**ehConf).load()
df = df.withColumn("body", df["body"].cast("string"))
#df.printSchema()

# COMMAND ----------

# MAGIC %fs ls  'abfss://ocio-dex-database@ocioededatalakedbr.dfs.core.windows.net/checkpoint/hl7_file_dropped_eh_raw'

# COMMAND ----------

db_name ="ocio_ede_dev"
tbl_name = "hl7_file_dropped_eh_raw"
schema_name = db_name + "." + tbl_name
#chkpoint_loc = "/tmp/delta/events/" + tbl_name + "/checkpoints/"
chkpoint_loc = 'abfss://ocio-dex-database@ocioededatalakedbr.dfs.core.windows.net/checkpoint/' + tbl_name
print(chkpoint_loc)

# COMMAND ----------

df.writeStream.format("delta").outputMode("append").option("checkpointLocation", chkpoint_loc).toTable(schema_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Count(*) FROM ocio_ede_dev.hl7_file_dropped_eh_raw;

# COMMAND ----------

# MAGIC %sql 
# MAGIC --Drop table ocio_ede_dev.hl7_file_dropped_eh_raw;

# COMMAND ----------

# MAGIC %fs
# MAGIC --rm -r 'abfss://ocio-dex-database@ocioededatalakedbr.dfs.core.windows.net/checkpoint/hl7_file_dropped_eh_raw'

# COMMAND ----------


