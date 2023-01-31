# Databricks notebook source
# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

dbutils.widgets.dropdown("input_database", "ocio_dex_dev", ["ocio_dex_dev"])

dbutils.widgets.dropdown("output_database", "ocio_dex_dev", ["ocio_dex_dev"])

dbutils.widgets.dropdown("output_checkpoint_prefix", "abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/events/", ["abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/events/"])

# COMMAND ----------

input_database =  dbutils.widgets.get("input_database")

output_database =  dbutils.widgets.get("output_database")

output_checkpoint_prefix = dbutils.widgets.get("output_checkpoint_prefix")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Database Config

# COMMAND ----------

class DatabaseConfig:
    
    def __init__(self, input_database, output_database, output_checkpoint_prefix):
        self.input_database = input_database
        self.output_database = output_database
        self.output_checkpoint_prefix = output_checkpoint_prefix
        
        
######################################################
# Populate from widgets
######################################################
database_config = DatabaseConfig(input_database, output_database, output_checkpoint_prefix)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table Config

# COMMAND ----------

class TableConfig:
    
    def __init__(self, database_config, topic, stage_in, stage_out):
        
        self.database_config = database_config
        self.topic = topic
        self.stage_in = stage_in
        self.stage_out = stage_out
    
    def input_database_table(self):
        return f"{self.database_config.input_database}.{self.topic}_{self.stage_in}"
    
    def output_database_table(self):
        return f"{self.database_config.output_database}.{self.topic}_{self.stage_out}"
    
    def output_checkpoint(self):
        return f"{self.database_config.output_checkpoint_prefix}{self.database_config.output_database}.{self.topic}_{self.stage_out}_checkpoint"   

# database_config = DatabaseConfig(environment)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lake Util (Read, Write)

# COMMAND ----------

class LakeUtil:
    
    def __init__(self, table_config):
        
        self.table_config = table_config
    
    def print_database_config(self):
        print(self.table_config.input_database_table())
        print(self.table_config.output_database_table())
        print(self.table_config.output_checkpoint())

     
    def read_stream_from_table(self):
        return spark.readStream.format("delta").option("ignoreDeletes", "true").table( self.table_config.input_database_table() )
    
    def write_stream_to_table(self, df):
        df.writeStream.format("delta").outputMode("append").trigger(availableNow=True).option("checkpointLocation", self.table_config.output_checkpoint() ).toTable( self.table_config.output_database_table() )
   

# COMMAND ----------

# MAGIC %md
# MAGIC ### Normalize String

# COMMAND ----------

def normalizeString(str):
    return str.replace("-", "_").lower()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Previous used LakeConfig, TODO: change to LakeUtil

# COMMAND ----------

class LakeConfig:
    def __init__(self, rootFolder, dbName):
        self.rootFolder = rootFolder
        self.dbName = dbName
        
        
    def getSchemaName(self, tableName):
        return self.dbName + "." + tableName
    
    def getCheckpointLocation(self, tableName):
        return self.rootFolder + "events/" + tableName + "/_checkpoint"
