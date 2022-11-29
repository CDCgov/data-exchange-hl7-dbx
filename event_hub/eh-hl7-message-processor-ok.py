# Databricks notebook source
import json, os
ev_namespace    ="tf-eventhub-namespace-dev"
ev_name         ="hl7-message-processor-ok"
ev_sas_key_name = os.getenv("v_eh_hl7_message_processor_ok_key")
ev_sas_key_val = os.getenv("v_eh_hl7_message_processor_ok_key_val")

conn_string="Endpoint=sb://{0}.servicebus.windows.net/;EntityPath={1};SharedAccessKeyName={2};SharedAccessKey={3}".format(ev_namespace, ev_name, ev_sas_key_name, ev_sas_key_val)

ehConf = {}
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(conn_string)
print(conn_string)

# COMMAND ----------

##### Creating an Event Hubs Source for Streaming Queries
df = spark.readStream.format("eventhubs").options(**ehConf).load()
df = df.withColumn("body", df["body"].cast("string"))
#df.printSchema()

# COMMAND ----------

db_name ="ocio_ede_dev"
tbl_name = "tbl_hl7_message_processor_ok"
schema_name = db_name + "." + tbl_name
chkpoint_loc = "/tmp/delta/events/hl7-message-processor-ok/_checkpoints/"

#df.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()
df.writeStream.format("delta").outputMode("append").option("checkpointLocation", chkpoint_loc).toTable(schema_name)

#df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM ocio_ede_dev.tbl_hl7_message_processor_ok;

# COMMAND ----------

# MAGIC %sql 
# MAGIC desc  table extended ocio_ede_dev.tbl_hl7_message_processor_ok;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ocio_ede_dev.tbl_hl7_message_processor_ok;

# COMMAND ----------


