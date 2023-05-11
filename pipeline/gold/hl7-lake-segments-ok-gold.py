# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook setting 

# COMMAND ----------

# MAGIC %run ../common/common_fns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports 

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Input Table

# COMMAND ----------

lakeDAO = LakeDAO(globalLakeConfig)
goldLakeDAO = LakeDAO(globalGOLDLakeConfig)

df1 = lakeDAO.readStreamFrom("hl7_lake_segments_ok_silver")

# COMMAND ----------

    
def transformAndSendToRoute(batchDF, batchId):
    routes_row_list = batchDF.select("message_info.route").distinct().collect() 
    routes_list = [x.route for x in routes_row_list]
    from functools import reduce
    for program_route in routes_list:
        # working through each batch of route
        # printToFile(TOPIC, "working on (start) route: -> " + str(program_route))
        # check if route == null, then push data into none table
        if program_route is None:
            df_one_route = batchDF.filter(col("message_info.route").isNull())
        else:    
            df_one_route = batchDF.filter( col("message_info.route") == program_route )

        # printToFile(TOPIC, f"records affected: {df_one_route.count()}")
        #printToFile(TOPIC, lake_util.get_for_print_gold_database_config( program_route ) )
        # lake_util.write_gold_to_table(df_one_route, program_route)
        goldLakeDAO.writeTableTo(df_one_route, f"{normalize(program_route)}_hl7_lake_segments_ok_gold")
        # working through each batch of route
        # printToFile(TOPIC, "working on (done) route: -> " + str(program_route))


# COMMAND ----------

#df1.writeStream.trigger(availableNow=True).foreachBatch( transformAndSendToRoute ).start()
df1.writeStream.trigger(availableNow=True).option("mergeSchema", "true") \
    .option("checkpointLocation", globalLakeConfig.getCheckpointLocation("hl7_lake_segments_ok_silver2gold_checkpoint")) \
    .foreachBatch( transformAndSendToRoute ).start()

# COMMAND ----------

# MAGIC %md
# MAGIC ### End
