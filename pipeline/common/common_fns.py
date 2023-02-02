# Databricks notebook source
# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

dbutils.widgets.dropdown("eventhub_namespace", "tf-eventhub-namespace-dev", ["tf-eventhub-namespace-dev"])

#

dbutils.widgets.dropdown("database", "ocio_dex_dev", ["ocio_dex_dev"])
dbutils.widgets.dropdown("database_checkpoint_prefix", "abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/events/", ["abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/events/"])
dbutils.widgets.dropdown("database_folder", "abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/", ["abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/"])

#

####### this can be used if final gold moves to Edav, etc..
dbutils.widgets.dropdown("gold_output_database", "ocio_dex_dev", ["ocio_dex_dev"])
dbutils.widgets.dropdown("gold_output_database_checkpoint_prefix", "abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/events/", ["abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/events/"])

# COMMAND ----------

eventhub_namespace =  dbutils.widgets.get("eventhub_namespace")
database =  dbutils.widgets.get("database")
database_checkpoint_prefix = dbutils.widgets.get("database_checkpoint_prefix")
database_folder = dbutils.widgets.get("database_folder")

gold_output_database =  dbutils.widgets.get("gold_output_database")
gold_output_database_checkpoint_prefix = dbutils.widgets.get("gold_output_database_checkpoint_prefix")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common Functions

# COMMAND ----------

def normalizeString(str):
    return str.replace("-", "_").lower()

def normalize(name):
    if name is not None:
        return name.replace(".", "_").replace(" ", "_").replace("'", "").lower()
    else:
        return str(name)
  
def printToFile(topic, message):
    import datetime
    file_loc = f"./{topic}-output-log.txt"
    with open(file_loc, "a") as f:
        f.write(f"{datetime.datetime.now()} - {message}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Database Config

# COMMAND ----------

class DatabaseConfig:
    
    def __init__(self, database, database_checkpoint_prefix, gold_output_database, gold_output_database_checkpoint_prefix):
        self.database = database
        self.database_checkpoint_prefix = database_checkpoint_prefix
        self.gold_output_database = gold_output_database
        self.gold_output_database_checkpoint_prefix = gold_output_database_checkpoint_prefix
        
        
######################################################
# Populate from widgets
######################################################
database_config = DatabaseConfig(database, database_checkpoint_prefix, gold_output_database, gold_output_database_checkpoint_prefix)

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
        return f"{self.database_config.database}.{self.topic}_{self.stage_in}"
    
    def output_database_table(self):
        return f"{self.database_config.database}.{self.topic}_{self.stage_out}"
    
    def output_checkpoint(self):
        return f"{self.database_config.database_checkpoint_prefix}{self.database_config.database}.{self.topic}_{self.stage_out}_checkpoint"   
      
    def output_gold_table(self, program_route):
        return f"{self.database_config.gold_output_database}.{normalize(program_route)}_{self.topic}_gold"
    
    def output_gold_table_checkpoint(self, program_route):
        output_gold_tbl = f"{self.database_config.gold_output_database}.{normalize(program_route)}_{self.topic}_gold"
        return  f"{gold_output_database_checkpoint_prefix}{output_gold_tbl}/_checkpoint" 
      
    def output_gold_repeat_table(self, program_route, repeat_table):
        gold_tbl = f"{self.database_config.gold_output_database}.{normalize(program_route)}_{self.topic}_gold"
        return f"{gold_tbl}_{repeat_table}"
    
    def output_gold_repeat_table_checkpoint(self, program_route, repeat_table):
        gold_tbl = f"{self.database_config.gold_output_database}.{normalize(program_route)}_{self.topic}_gold"
        return f"{gold_output_database_checkpoint_prefix}{gold_tbl}_{repeat_table}/_checkpoint" 


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
        
    def print_gold_database_config(self, program_route):
        print(self.table_config.output_gold_table(program_route))
        print(self.table_config.output_gold_table_checkpoint(program_route))
        
    def get_for_print_gold_database_config(self, program_route):
        return f"table: { self.table_config.output_gold_table(program_route) } - checkpoint: { self.table_config.output_gold_table_checkpoint(program_route) }"
    
    def print_gold_database_repeat_config(self, program_route, repeat_table):
        print(self.table_config.output_gold_repeat_table(program_route, repeat_table))
        print(self.table_config.output_gold_repeat_table_checkpoint(program_route, repeat_table))
    
    def get_for_print_gold_database_repeat_config(self, program_route, repeat_table):
        return f"table: { self.table_config.output_gold_repeat_table(program_route, repeat_table) } - checkpoint: { self.table_config.output_gold_repeat_table_checkpoint(program_route, repeat_table) }"

    def read_stream_from_table(self):
        return spark.readStream.format("delta").option("ignoreDeletes", "true").table( self.table_config.input_database_table() )
    
    def write_stream_to_table(self, df):
        df.writeStream.format("delta").outputMode("append").trigger(availableNow=True).option("checkpointLocation", self.table_config.output_checkpoint() ).toTable( self.table_config.output_database_table() )
    
    def write_gold_to_table(self, df, program_route):
        df.write.format("delta").mode("append") \
        .option("checkpointLocation", self.table_config.output_gold_table_checkpoint(program_route) ) \
        .saveAsTable( self.table_config.output_gold_table(program_route) )
    
    def write_gold_repeat_to_table(self, df, program_route, repeat_table):
        #TODO determine if checkpoints are needed here, or should move to the writeStream in the notebooks which should be configured then.
        df.write.format("delta").mode("append") \
        .option("checkpointLocation", self.table_config.output_gold_repeat_table_checkpoint(program_route, repeat_table) ) \
        .saveAsTable( self.table_config.output_gold_repeat_table(program_route, repeat_table) )

# this is not used since all notebooks use foreachbatch so using write_gold_to_table
#     def write_gold_stream_to_table(self, df, program_route):
#         df.writeStream.format("delta").outputMode("append").trigger(availableNow=True).option("checkpointLocation", self.table_config.output_gold_table_checkpoint(program_route) ).toTable( self.table_config.output_gold_table(program_route) )
        

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Class to get ReadStream on a table

# COMMAND ----------

class getTableStream:
    def __init__(self, database_config,tableName):
        self.database_config = database_config
        self.tableName = tableName
        
    def input_database_table(self):
        return f"{self.database_config.database}.{self.tableName}"    
        
    def getReadStream(self):
        return spark.readStream.format("delta").option("ignoreDeletes", "true").table(self.input_database_table())
