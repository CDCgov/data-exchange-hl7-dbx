# Databricks notebook source
import os

class DEXEnvironment:
    eventhub_namespace = dbutils.jobs.taskValues.get(taskKey = "set_job_params", key = "eventhub_namespace", debugValue = os.getenv("eventhub_namespace"))
    database = dbutils.jobs.taskValues.get(taskKey = "set_job_params", key = "database", debugValue = os.getenv("database"))
    #database_checkpoint_prefix = dbutils.jobs.taskValues.get(taskKey = "set_job_params", key = "database_checkpoint_prefix", debugValue = os.getenv("database_checkpoint_prefix"))
    database_folder = dbutils.jobs.taskValues.get(taskKey = "set_job_params", key = "database_folder", debugValue = os.getenv("database_folder"))
    scope_name = dbutils.jobs.taskValues.get(taskKey = "set_job_params", key = "scope_name", debugValue = os.getenv("scope_name"))
    gold_output_database = dbutils.jobs.taskValues.get(taskKey = "set_job_params", key = "gold_output_database", debugValue = os.getenv("gold_output_database"))
    #gold_output_database_checkpoint_prefix = dbutils.jobs.taskValues.get(taskKey = "set_job_params", key = "gold_output_database_checkpoint_prefix", debugValue = os.getenv("gold_output_database_checkpoint_prefix"))
    gold_database_folder = dbutils.jobs.taskValues.get(taskKey = "set_job_params", key = "gold_database_folder", debugValue = os.getenv("gold_database_folder"))

globalDexEnv = DEXEnvironment()


# COMMAND ----------

class LakeConfig:
    def __init__(self, dbName, rootFolder):
        self.dbName = dbName
        self.rootFolder = rootFolder
    
    def getTableRef(self, tableName):
        return f"{self.dbName}.{tableName}"
    
    def getCheckpointLocation(self, tableName):
        return f"{self.rootFolder}/checkpoints/{tableName}"
    
    
class LakeDAO:
    def __init__(self, lakeConfig):
        self.lakeConfig = lakeConfig
        
    def readStreamFrom(self, tableName):
        return spark.readStream.format("delta").option("ignoreDeletes", "true").table( self.lakeConfig.getTableRef(tableName) )    
    
    def writeStreamTo(self, df, tableName):
        return df.writeStream.format("delta").outputMode("append") \
            .trigger(availableNow=True)  \
            .option("checkpointLocation", self.lakeConfig.getCheckpointLocation(tableName)).toTable(self.lakeConfig.getTableRef(tableName))
    
    def writeTableTo(self, df, tableName):
        return df.write.format("delta").mode("append").option("mergeSchema", "true") \
            .saveAsTable( self.lakeConfig.getTableRef(tableName) )
        
class EventHubConfig:
    def __init__(self, namespace, scope):
        self.namespace = namespace
        self.scope = scope
        self.sasKey =   dbutils.secrets.get(scope=self.scope, key="dbx-eh-namespace-key-name")
        self.sasValue = dbutils.secrets.get(scope=self.scope, key="dbx-eh-namespace-key-val")
        
    def connString(self, topic): 
        return "Endpoint=sb://{0}.servicebus.windows.net/;EntityPath={1};SharedAccessKeyName={2};SharedAccessKey={3}".format(self.namespace, topic, self.sasKey, self.sasValue)   
    
    def getConfig(self, topic):
        ehConf = {}
        ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(self.connString(topic))
        
        # TODO: keep name in sync with infra
        ehConf['eventhubs.consumerGroup'] = "dbx-" + topic + "-cg-001"
        return ehConf    

# COMMAND ----------

# MAGIC %md # Export Global Objects - globalLakeConfig, globalEventHubConfig

# COMMAND ----------

globalLakeConfig = LakeConfig(globalDexEnv.database, globalDexEnv.database_folder)   
globalGOLDLakeConfig = LakeConfig(globalDexEnv.gold_output_database, globalDexEnv.gold_database_folder)

globalEventHubConfig = EventHubConfig(globalDexEnv.eventhub_namespace, globalDexEnv.scope_name)

# COMMAND ----------

# MAGIC %md # Creating Widgets for different Event Hubs

# COMMAND ----------

dbutils.widgets.dropdown("event_hub", "hl7-recdeb-ok", ["hl7-file-dropped-ok", "hl7-recdeb-ok", "hl7-recdeb-err", "hl7-redacted-ok", "hl7-redacted-err",  "hl7-structure-ok", "hl7-structure-elr-ok" "hl7-structure-err", "hl7-mmg-validation-ok", "hl7-mmg-validation-err", "hl7-mmg-based-ok", "hl7-mmg-based-err", "hl7-mmg-sql-ok", "hl7-mmg-sql-err", "hl7-lake-segments-ok", "hl7-lake-segments-err","hl7-json-lake-ok","hl7-json-lake-err"])
