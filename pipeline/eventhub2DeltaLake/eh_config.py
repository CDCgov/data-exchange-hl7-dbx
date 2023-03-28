# Databricks notebook source
# MAGIC %run ../common/common_fns

# COMMAND ----------

# DBTITLE 1,Class to hold Event Hub configuration 
import json, os

class EventHubConfig:
    def __init(self, namespace, scope, topic):
        self.namespace = namespace
        self.sasKey =   dbutils.secrets.get(scope=scope_name, key="dbx-eh-namespace-key-name")
        self.sasValue = dbutils.secrets.get(scope=scope_name, key="dbx-eh-namespace-key-val")
        self.topic = topic

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
        
        # TODO: keep name in sync with infra
        ehConf['eventhubs.consumerGroup'] = "dbx-" + self.topic + "-cg-001"
        return ehConf

# COMMAND ----------

# TEST
# eh = EventHubConfig("namespace", "topic", "sasKey", "pwd-123")
# print(eh.getConfig())

# COMMAND ----------

def _transferEventHubDataToLake(eventHubConfig, lakeConfig):
    ehConfig = eventHubConfig.getConfig()
    df = spark.readStream.format("eventhubs").options(**ehConfig).load()
    df = df.withColumn("body", df["body"].cast("string"))
    
    # Standardize on Table names for Event Hub topics:
    tbl_name = normalizeString(eventHubConfig.topic) + "_eh_raw"
  
    lakeDAO = LakeDAO(lakeConfig)
    lakeDAO.writeStreamTo(df, tbl_name)


# COMMAND ----------

# DBTITLE 1,Opinionated method that knows the Event Hub, and Lake configurations. All it needs is what Topic to load!
def transferEventHubDataToLake(eventHubTopic, lakeConfig, eventHubConfig):
      ehConfig = EventHubConfig(ev_namespace, scope_name, eventHubTopic)
    
    _transferEventHubDataToLake(ehConfig, lakeConfig)

# COMMAND ----------

## Pull data from Eventhub to Delta Lake
#eventHubTopic = "hl7-file-dropped"
#transferEventHubDataToLake(eventHubTopic)

# COMMAND ----------

# MAGIC %sql 
# MAGIC --Drop table ocio_ede_dev.hl7_file_dropped_eh_raw_test

# COMMAND ----------


