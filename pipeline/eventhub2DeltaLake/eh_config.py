# Databricks notebook source
# MAGIC %md
# MAGIC This notebook provides the basic classes to configure Event Hub and Lake configs.
# MAGIC Those two classes are used by the transferEventHubDataToLake method - it will read the appropriate topic from the Event Hub namespace and write its content to the given table.

# COMMAND ----------

# DBTITLE 1,Class to hold Event Hub configuration 
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
        return ehConf

# COMMAND ----------

# TEST
# eh = EventHubConfig("namespace", "topic", "sasKey", "pwd-123")
# print(eh.getConfig())

# COMMAND ----------

# DBTITLE 1,Class to hold Lake configuration
class LakeConfig:
    def __init__(self, rootFolder, dbName, tableName):
        self.rootFolder = rootFolder
        self.dbName = dbName
        self.tableName = tableName
        
    def getSchemaName(self):
        return self.dbName + "." + self.tableName
    
    def getCheckpointLocation(self):
        return self.rootFolder + "events/" + self.tableName + "/_checkpoint"

# COMMAND ----------

def normalizeString(str):
    return str.replace("-", "_").lower()

# COMMAND ----------

def _transferEventHubDataToLake(eventHubConfig, lakeConfig):
    ehConfig = eventHubConfig.getConfig()
    df = spark.readStream.format("eventhubs").options(**ehConfig).load()
    df = df.withColumn("body", df["body"].cast("string"))
    
    # Standardize on Table names for Event Hub topics:
#     lakeConfig.tableName = "tbl_bronze_" + eventHubConfig.topic
    
    df.writeStream.format("delta").outputMode("append").option("checkpointLocation", lakeConfig.getCheckpointLocation()).toTable(lakeConfig.getSchemaName())

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
 ##   tbl_name = "tbl_bronze_" + normalizeString(eventHubTopic)
    tbl_name = normalizeString(eventHubTopic) + "_eh_raw"
 ##  root_folder = "/tmp/delta/"
    root_folder = 'abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/' 
    
    lakeConfig = LakeConfig(root_folder, db_name, tbl_name) 
    _transferEventHubDataToLake(ehConfig, lakeConfig)

# COMMAND ----------

## Pull data from Eventhub to Delta Lake
#eventHubTopic = "hl7-file-dropped"
#transferEventHubDataToLake(eventHubTopic)

# COMMAND ----------

# MAGIC %sql 
# MAGIC --Drop table ocio_ede_dev.hl7_file_dropped_eh_raw_test

# COMMAND ----------


