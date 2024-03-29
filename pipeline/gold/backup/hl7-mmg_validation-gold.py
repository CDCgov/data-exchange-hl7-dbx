# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook setting 

# COMMAND ----------

TOPIC = "hl7_mmg_validation"
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

    
def transformAndSendToRoute(batchDF, batchId):
    routes_row_list = batchDF.select("message_info.route").distinct().collect() 
    routes_list = [x.route for x in routes_row_list]

    for program_route in routes_list:
        # working through each batch of route
     #   printToFile(TOPIC, "working on (start) route: -> " + str(program_route))
        # check if route == null, then push data into none table
        if program_route is None:
            df_one_route = batchDF.filter(col("message_info.route").isNull())
        else:    
            df_one_route = batchDF.filter( col("message_info.route") == program_route )
        
   #     printToFile(TOPIC, f"records affected: {df_one_route.count()}")
   #     printToFile(TOPIC, lake_util.get_for_print_gold_database_config( program_route ) )
        lake_util.write_gold_to_table(df_one_route, normalize(program_route))
        
        # working through each batch of route
    #    printToFile(TOPIC, "working on (done) route: -> " + str(program_route))


# COMMAND ----------

#spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")

# COMMAND ----------

#df1.writeStream.trigger(availableNow=True).foreachBatch( transformAndSendToRoute ).start()
df1.writeStream.trigger(availableNow=True).option("mergeSchema", "true") \
    .option("checkpointLocation", f"{globalDexEnv.database_checkpoint_prefix}/checkpoints/hl7_mmg_validation_silver2gold_checkpoint") \
    .foreachBatch( transformAndSendToRoute ).start()

# COMMAND ----------

# MAGIC %md
# MAGIC ### End
