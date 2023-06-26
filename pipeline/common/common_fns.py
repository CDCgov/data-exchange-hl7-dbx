# Databricks notebook source
import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

# dbutils.widgets.dropdown("eventhub_namespace", "tf-eventhub-namespace-dev", ["tf-eventhub-namespace-dev"])

# #
# #dbutils.widgets.dropdown("scope_name", "dbs-scope-DEX", ["dbs-scope-DEX"])
# dbutils.widgets.dropdown("scope_name", "DBS-SCOPE-DEX-DEV", ["DBS-SCOPE-DEX-DEV"])
# dbutils.widgets.dropdown("database", "ocio_dex_dev", ["ocio_dex_dev"])
# dbutils.widgets.dropdown("database_checkpoint_prefix", "abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/checkpoints", ["abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/checkpoints"])
# dbutils.widgets.dropdown("database_folder", "abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta", ["abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta"])

# #
# ####### this can be used if final gold moves to Edav, etc..
# dbutils.widgets.dropdown("gold_output_database", "ocio_edav_dex_dev", ["ocio_edav_dex_dev"])
# dbutils.widgets.dropdown("gold_output_database_checkpoint_prefix", "abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/checkpoints", ["abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/checkpoints"])
# dbutils.widgets.dropdown("gold_database_folder", "abfss://database@edavdevdatalakedex.dfs.core.windows.net/delta/", ["abfss://database@edavdevdatalakedex.dfs.core.windows.net/delta/"])


# COMMAND ----------

# eventhub_namespace =  dbutils.widgets.get("eventhub_namespace")
# database =  dbutils.widgets.get("database")
# database_checkpoint_prefix = dbutils.widgets.get("database_checkpoint_prefix")
# database_folder = dbutils.widgets.get("database_folder")

# scope_name= dbutils.widgets.get("scope_name")
# gold_output_database =  dbutils.widgets.get("gold_output_database")
# gold_output_database_checkpoint_prefix = dbutils.widgets.get("gold_output_database_checkpoint_prefix")
# gold_database_folder = dbutils.widgets.get("gold_database_folder")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task Values

# COMMAND ----------

# MAGIC %run ./setup_env

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common Functions

# COMMAND ----------

def normalizeString(str):
    return str.replace("-", "_").lower()

def normalize(name):
    if name is not None:
        return name.replace(".", "_").replace(" ", "_").replace("'", "").replace("-", "_").lower()
    else:
        return str(name)

# TODO: move potentially to environment var
#debugToFileIsEnabled = True
debugToFileIsEnabled = False
def printToFile(topic, message):
  if debugToFileIsEnabled and message:
      import datetime
      file_loc = f"./{topic}-output-log.txt"
      with open(file_loc, "a") as f:
          f.write(f"{datetime.datetime.now()} - {message}\n")

def lake_metadata_create(topic,df,action_type):
    timestamp = datetime.datetime.now()
    options = ["insert","append"]
    
    if action_type not in options:
        return print('Action type needs to be: "insert" or "append')
    

    json_str = f'''{{"process_name":"{topic}","created_timestamp":"{timestamp}"}}'''
    df = df.withColumn("json_str",lit(json_str))
    df = df.withColumn("json_str",from_json("json_str",schema_lake_metadata_processes))

    if action_type.lower() == 'insert':
        df = df.withColumn("lake_metadata",struct(array(col("json_str")).alias("processes")))
        
    elif action_type.lower() == 'append':
         df = df.withColumn("lake_metadata",struct(array_union(col("lake_metadata.processes"),array(col("json_str"))).alias("processes")))

    df = df.drop(col("json_str"))
    
    return df

# COMMAND ----------

# MAGIC %md # Every CELL BELOW this shall be removed once Refactorig is complete!

# COMMAND ----------

# import os


# eventhub_namespace = dbutils.jobs.taskValues.get(taskKey = "set_job_params", key = "eventhub_namespace", debugValue = os.getenv("eventhub_namespace"))
# database = dbutils.jobs.taskValues.get(taskKey = "set_job_params", key = "database", debugValue = os.getenv("database"))
# database_checkpoint_prefix = dbutils.jobs.taskValues.get(taskKey = "set_job_params", key = "database_checkpoint_prefix", debugValue = os.getenv("database_checkpoint_prefix"))
# database_folder = dbutils.jobs.taskValues.get(taskKey = "set_job_params", key = "database_folder", debugValue = os.getenv("database_folder"))
# scope_name = dbutils.jobs.taskValues.get(taskKey = "set_job_params", key = "scope_name", debugValue = os.getenv("scope_name"))
# gold_output_database = dbutils.jobs.taskValues.get(taskKey = "set_job_params", key = "gold_output_database", debugValue = os.getenv("gold_output_database"))
# gold_output_database_checkpoint_prefix = dbutils.jobs.taskValues.get(taskKey = "set_job_params", key = "gold_output_database_checkpoint_prefix", debugValue = os.getenv("gold_output_database_checkpoint_prefix"))
# gold_database_folder = dbutils.jobs.taskValues.get(taskKey = "set_job_params", key = "gold_database_folder", debugValue = os.getenv("gold_database_folder"))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Database Config

# COMMAND ----------

# class DatabaseConfig:
    
#     def __init__(self, database, database_checkpoint_prefix, gold_output_database, gold_output_database_checkpoint_prefix):
#         self.database = database
#         self.database_checkpoint_prefix = database_checkpoint_prefix
#         self.gold_output_database = gold_output_database
#         self.gold_output_database_checkpoint_prefix = gold_output_database_checkpoint_prefix
        
        
######################################################
# Populate from widgets
######################################################
# database_config = DatabaseConfig(globalDexEnv.database, globalDexEnv.database_checkpoint_prefix, globalDexEnv.gold_output_database, globalDexEnv.gold_output_database_checkpoint_prefix)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table Config

# COMMAND ----------

# class TableConfig:
    
#     def __init__(self, database_config, topic, stage_in, stage_out):
        
#         self.database_config = database_config
#         self.topic = topic
#         self.stage_in = stage_in
#         self.stage_out = stage_out
    
#     def input_database_table(self):
#         return f"{self.database_config.database}.{self.topic}_{self.stage_in}"
    
#     def output_database_table(self):
#         return f"{self.database_config.database}.{self.topic}_{self.stage_out}"
    
#     def output_gold_table(self, program_route):
#         return f"{self.database_config.gold_output_database}.{normalize(program_route)}_{self.topic}_gold"
    
#     def output_gold_repeat_table(self, program_route, repeat_table):
#         return f"{self.database_config.gold_output_database}.{normalize(program_route)}_{repeat_table}_{self.topic}_gold"
        
#     def output_checkpoint(self):
#         return f"{self.database_config.database_checkpoint_prefix}/{self.topic}_{self.stage_out}_checkpoint"   
      
#     def output_gold_table_checkpoint(self, program_route):
#         return  f"{self.database_config.gold_output_database_checkpoint_prefix}/{normalize(program_route)}_{self.topic}_gold_checkpoint" 
      
#     def output_gold_repeat_table_checkpoint(self, program_route, repeat_table):
#         return f"{self.database_config.gold_output_database_checkpoint_prefix}/{normalize(program_route)}_{repeat_table}_{self.topic}_gold_checkpoint" 


# COMMAND ----------

# MAGIC %md
# MAGIC ### Lake Util (Read, Write)

# COMMAND ----------

# class LakeUtil:
    
#     def __init__(self, table_config):
        
#         self.table_config = table_config
        
# #     def get_for_print_gold_database_config(self, program_route):
# #         return f"table: { self.table_config.output_gold_table(program_route) } - checkpoint: { self.table_config.output_gold_table_checkpoint(program_route) }"
    
# #     def get_for_print_gold_database_repeat_config(self, program_route, repeat_table):
# #         return f"table: { self.table_config.output_gold_repeat_table(program_route, repeat_table) } - checkpoint: { self.table_config.output_gold_repeat_table_checkpoint(program_route, repeat_table) }"

#     def read_stream_from_table(self):
#         return spark.readStream.format("delta").option("ignoreDeletes", "true").table( self.table_config.input_database_table() )
    
#     def write_stream_to_table(self, df):
#         df.writeStream.format("delta").outputMode("append").option("mergeSchema", "true").trigger(availableNow=True).option("checkpointLocation", self.table_config.output_checkpoint() ).toTable( self.table_config.output_database_table() )
    
#     def write_gold_to_table(self, df, program_route):
#         df.write.format("delta").mode("append").option("mergeSchema", "true") \
#         .saveAsTable( self.table_config.output_gold_table(program_route) )
    
#     def write_gold_repeat_to_table(self, df, program_route, repeat_table):
#         #TODO determine if checkpoints are needed here, or should move to the writeStream in the notebooks which should be configured then.
#         df.write.format("delta").mode("append").option("mergeSchema", "true") \
#         .saveAsTable( self.table_config.output_gold_repeat_table(program_route, repeat_table) )
       

# COMMAND ----------

# #used to get readStream on a table
# def getTableStream(database_config,tbl_name):
#         tblName = database_config.database+"."+tbl_name
#         return spark.readStream.format("delta").option("ignoreDeletes", "true").table(tblName)       


# COMMAND ----------

# # used for raw tables
# def writeStreamToTable(database_config,tbl_name,df):
#     checkpt = f"{database_config.database_checkpoint_prefix}/{tbl_name}_checkpoint"
#     dbname = database_config.database+"."+tbl_name 
#     df.writeStream.format("delta").outputMode("append").trigger(availableNow=True).option("checkpointLocation", checkpt).toTable(dbname)
