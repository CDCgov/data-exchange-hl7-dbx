# Databricks notebook source
# MAGIC %run ../common/common_fns

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
        
        # TODO: keep name in sync with infra
        ehConf['eventhubs.consumerGroup'] = "dbx-" + self.topic + "-cg-001"
        return ehConf

# COMMAND ----------

# TEST
# eh = EventHubConfig("namespace", "topic", "sasKey", "pwd-123")
# print(eh.getConfig())

# COMMAND ----------

def _transferEventHubDataToLake(eventHubConfig):
    ehConfig = eventHubConfig.getConfig()
    df = spark.readStream.format("eventhubs").options(**ehConfig).load()
    df = df.withColumn("body", df["body"].cast("string"))
    
    
    # Standardize on Table names for Event Hub topics:
#     lakeConfig.tableName = "tbl_bronze_" + eventHubConfig.topic
    tbl_name = normalizeString(eventHubConfig.topic) + "_eh_raw"
    checkpt = f"{database_config.database_checkpoint_prefix}{database_config.database}.{tbl_name}_checkpoint"
    dbname = database_config.database+"."+tbl_name
  
    writeStreamToTable(database_config,tbl_name,df)
    #df.writeStream.format("delta").outputMode("append").trigger(availableNow=True).option("checkpointLocation", checkpt).toTable(dbname)
  

# COMMAND ----------

# DBTITLE 1,Opinionated method that knows the Event Hub, and Lake configurations. All it needs is what Topic to load!
def transferEventHubDataToLake(eventHubTopic):

    # from other notebook (at top) from widgets
    ev_namespace = eventhub_namespace
    db_name =  database
    root_folder =  database_folder
    
    ## Scope defined by EDAV team
   ## scope_name = "dbs-scope-dex"
    #scope_name = "ocio-ede-dev-vault"
    key_name = "tf-eh-namespace-key"
    key_val = "tf-eh-namespace-key-val"

    ev_sas_key_name = dbutils.secrets.get(scope=scope_name, key=key_name)
    ev_sas_key_val = dbutils.secrets.get(scope=scope_name, key=key_val)

### Creating Connnection String 
    ehConfig = EventHubConfig(ev_namespace, eventHubTopic, ev_sas_key_name, ev_sas_key_val)
    
#     db_name  = "ocio_dex_dev"
 ##  root_folder = "/tmp/delta/"
#     root_folder = 'abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/' 
    
    #lakeConfig = LakeConfig(root_folder, db_name) 
    _transferEventHubDataToLake(ehConfig)

# COMMAND ----------

## Pull data from Eventhub to Delta Lake
#eventHubTopic = "hl7-file-dropped"
#transferEventHubDataToLake(eventHubTopic)

# COMMAND ----------

# MAGIC %sql 
# MAGIC --Drop table ocio_ede_dev.hl7_file_dropped_eh_raw_test

# COMMAND ----------


