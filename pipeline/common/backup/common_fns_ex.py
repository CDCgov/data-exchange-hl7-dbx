# Databricks notebook source
# MAGIC %md # External Environment Variables
# MAGIC 
# MAGIC This notebook captures the external variables that are needed to persist and read data and provide some helper classes to encapsulate environment specific functionality.
# MAGIC 
# MAGIC The parameters expected are:
# MAGIC * **dbName** - Name of the Database on a given environment
# MAGIC * **storage** - Storage name to use for persisting delta data and reading ingress data and config files.
# MAGIC * **container** - Container name used by this project.
# MAGIC 
# MAGIC The implementation is very opinionated. It expects the following folder structure on the container described above:
# MAGIC * **/ingress/** - Root folder to store ingress files. This folder may contain subfolders by file format or source data. The subfolders are passed by the notebook implementing the ingress logic.
# MAGIC * **/config/**  - Root folder to store any configuration file needed by the notebooks on this project.
# MAGIC * **/delta/**   - Root folder to store delta files - tables and checkpoints. 
# MAGIC   * **/delta/database/** - Root folder to store database data. under this folder, there will be subfolders with dbName and tables will be stored under that.
# MAGIC   * **/delta/checkpoints/** - Root folder to store checkpoint files for a given table. Under this folder, there will be subfolders with dbName and table names accordingly.

# COMMAND ----------

dbutils.widgets.text("dbName", "ocio_dex_dev")
dbutils.widgets.text("storage", "ocioededatalakedbr")
dbutils.widgets.text("container", "ocio-dex-db-dev")

dbutils.widgets.text("event_hub_ns", "tf-eventhub-namespace-dev")

# COMMAND ----------

class Env:
    def __init__(self):
        self.dbName = dbutils.widgets.get("dbName")
        self.storage = dbutils.widgets.get("storage")
        self.container = dbutils.widgets.get("container")
        self.eventHubNS = dbutils.widgets.get("event_hub_ns")

        self.mntIngress = f"abfss://{self.container}@{self.storage}.dfs.core.windows.net/ingress/"
        self.mntConfig =  f"abfss://{self.container}@{self.storage}.dfs.core.windows.net/config/"
        self.mntDelta  =  f"abfss://{self.container}@{self.storage}.dfs.core.windows.net/delta/"

    def getDeltaFolder(self):
        return self.mntDelta
    def getDatabaseName(self):
        return self.dbName
    def getStorageAcct(self):
        return self.storage
    def getEventHubNamespace(self):
        return self.eventHubNS

# COMMAND ----------

class LakeConfig:
    def __init__(self, environment):
        self.env = environment
              
    def getSchemaName(self, tableName):
        return self.env.getDatabaseName() + "." + tableName
    
    def getCheckpointLocation(self, tableName):
        return self.env.getDeltaFolder() + self.env.getDatabaseName() + "/" + tableName + "/_checkpoint"

# COMMAND ----------

def normalizeString(str):
    return str.replace("-", "_").lower()
