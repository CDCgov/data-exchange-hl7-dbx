# Databricks notebook source
# MAGIC %md
# MAGIC This notebook provides the basic classes to configure Event Hub and Lake configs.
# MAGIC Those two classes are used by the transferEventHubDataToLake method - it will read the appropriate topic from the Event Hub namespace and write its content to the given table.

# COMMAND ----------

# DBTITLE 1,Class to hold Event Hub configuration 
import json, os

class EventHubConfig:
    def __init__(self, namespace, topic, sasKey, sasValue):
        self.namespace = namespace
        self.topic = topic
        self.sasKey = sasKey
        self.sasValue = sasValue
        
    def connString(self): 
        return "Endpoint=sb://{0}.servicebus.windows.net/;EntityPath={1};SharedAccessKeyName={2};SharedAccessKey={3}".format(self.namespace, self.topic, self.sasKey, self.sasValue)   
    
    def getConfig(self):
        ehConf = {}
        ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(self.connString())
        ehConf['eventhubs.consumerGroup'] = "dbx-" + self.topic + "-cg-001"
        return ehConf

# COMMAND ----------

# TEST
# eh = EventHubConfig("namespace", "topic", "sasKey", "pwd-123")
# print(eh.getConfig())

# COMMAND ----------

# MAGIC %run ../common/common_fns

# COMMAND ----------

def _transferEventHubDataToLake(eventHubConfig, lakeConfig):
    ehConfig = eventHubConfig.getConfig()
    df = spark.readStream.format("eventhubs").options(**ehConfig).load()
    df = df.withColumn("body", df["body"].cast("string"))
    
    # Standardize on Table names for Event Hub topics:
#     lakeConfig.tableName = "tbl_bronze_" + eventHubConfig.topic
    tbl_name = normalizeString(eventHubConfig.topic) + "_eh_raw"
    df.writeStream.format("delta").outputMode("append").trigger(availableNow=True).option("checkpointLocation", lakeConfig.getCheckpointLocation(tbl_name)).toTable(lakeConfig.getSchemaName(tbl_name))

# COMMAND ----------

# DBTITLE 1,Opinionated method that knows the Event Hub, and Lake configurations. All it needs is what Topic to load!
def transferEventHubDataToLake(eventHubTopic):
    ev_namespace    = "tf-eventhub-namespace-dev"
    ev_sas_key_name = os.getenv("v_tf_eventhub_namespace_dev_key")
    ev_sas_key_val  = os.getenv("v_tf_eventhub_namespace_dev_key_val")

#    ev_sas_key_name = "tf-eventhub-namespace-dev-key"
#    ev_sas_key_val  = os.getenv("event-hup-policy-key")

### Creating Connnection String 
    ehConfig = EventHubConfig(ev_namespace, eventHubTopic, ev_sas_key_name, ev_sas_key_val)
    
    db_name  = "ocio_dex_dev"
 ##  root_folder = "/tmp/delta/"
    root_folder = 'abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/' 
    
    lakeConfig = LakeConfig(root_folder, db_name) 
    _transferEventHubDataToLake(ehConfig, lakeConfig)

# COMMAND ----------

## Pull data from Eventhub to Delta Lake
#eventHubTopic = "hl7-file-dropped"
#transferEventHubDataToLake(eventHubTopic)

# COMMAND ----------

# MAGIC %sql 
# MAGIC --Drop table ocio_ede_dev.hl7_file_dropped_eh_raw_test

# COMMAND ----------


