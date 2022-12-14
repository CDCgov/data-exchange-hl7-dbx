# Databricks notebook source
import json, os
ev_namespace    ="tf-eventhub-namespace-dev"
ev_name         ="hl7-file-dropped"
ev_sas_key_name = os.getenv("v_hl7_file_dropped_key")
ev_sas_key_val = os.getenv("v_hl7_file_dropped_key_val")

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
tbl_name = "tbl_hl7_file_dropped"
schema_name = db_name + "." + tbl_name
chkpoint_loc = "/tmp/delta/events/hl7_file_dropped/_checkpoints/"

df.writeStream.format("delta").outputMode("append").option("checkpointLocation", chkpoint_loc).toTable(schema_name)
#df.writeStream.format("delta").outputMode("append").option("checkpointLocation", "/tmp/delta/events/_checkpoints/").start("/delta/events")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Count(*) FROM ocio_ede_dev.tbl_hl7_file_dropped;

# COMMAND ----------

db_name ="ocio_ede_dev"
tbl_name = "tbl_hl7_file_dropped"
schema_name = db_name + "." + tbl_name
chkpoint_loc = "/tmp/delta/events/" +tbl_name + "/_checkpoints/"
print(chkpoint_loc)

# COMMAND ----------

hl7StructureErrTableName = "ocio_ede_dev.tbl_hl7_structure_err"
df =  spark.read.format("delta").table(hl7StructureErrTableName)
df2 = spark.sql('''SELECT cast(body AS STRING) FROM ocio_ede_dev.tbl_hl7_structure_err ''')
#df2 = df.select("body")
#df3 = df2.withColumn("body", df["body"].cast("to_json"))
#df3 = df2.to_json(df2)
display(df2)

# COMMAND ----------

# MAGIC %sql 
# MAGIC --Drop table ocio_ede_dev.tbl_hl7_file_dropped;
