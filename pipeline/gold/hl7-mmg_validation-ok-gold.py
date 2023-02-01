# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook setting 

# COMMAND ----------

TOPIC = "hl7_mmg_validation_ok"
STAGE_IN = "silver"
STAGE_OUT = "gold"

# COMMAND ----------

# MAGIC %run ../common/common_fns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports 

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Input and Output Tables

# COMMAND ----------

lake_util = LakeUtil( TableConfig(database_config, TOPIC, STAGE_IN, STAGE_OUT) )

# test check print gold database_config
# print( lake_util.print_database_config() )
# print( lake_util.print_gold_database_config("myprogramroute") )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Input Table

# COMMAND ----------

df1 = lake_util.read_stream_from_table()

# COMMAND ----------

def printToFile(message):
    import datetime
    with open("./mmg-validation-ok-gold-output-log.txt", "a") as f:
        f.write(f"{datetime.datetime.now()} - {message}\n")

def normalize(name):
    if name is not None:
        return name.replace(".", "_").replace(" ", "_").replace("'", "")
    else:
        return str(name)
    
def transformAndSendToRoute(batchDF, batchId):
    routes_row_list = batchDF.select("message_info.route").distinct().collect() 
    routes_list = [x.route for x in routes_row_list]

    for program_route in routes_list:
        # working through each batch of route
        printToFile("working on (start) route: -> " + str(program_route))
        # check if route == null, then push data into none table
        if(program_route == 'None'):
            df_one_route = batchDF.filter(col("message_info.route").isNull())
        else:    
            df_one_route = batchDF.filter( col("message_info.route") == program_route )
            
        printToFile( lake_util.print_gold_database_config( program_route ) )
        lake_util.write_gold_to_table(df_one_route, program_route)
        
        # working through each batch of route
        printToFile("working on (done) route: -> " + str(program_route))


# COMMAND ----------

df1.writeStream.trigger(availableNow=True).foreachBatch( transformAndSendToRoute ).start()

# COMMAND ----------

# MAGIC %md
# MAGIC ### End
