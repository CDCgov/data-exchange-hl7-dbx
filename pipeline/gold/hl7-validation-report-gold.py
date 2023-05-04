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
from functools import reduce

# COMMAND ----------

# MAGIC %md
# MAGIC ### Input and Output Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Input Table

# COMMAND ----------

lakeDAO = LakeDAO(globalLakeConfig)
goldLakeDAO = LakeDAO(globalGOLDLakeConfig)

df1 = lakeDAO.readStreamFrom("hl7_validation_report_silver")

# COMMAND ----------

    
def transformAndSendToRoute(batchDF, batchId):
    routes_row_list = batchDF.select("message_info.route").distinct().collect() 
    routes_list = [x.route for x in routes_row_list]
    
    for program_route in routes_list:
        # check if route == null, then push data into none table
        if program_route is None:
            df_one_route = batchDF.filter(col("message_info.route").isNull())
        else:    
            df_one_route = batchDF.filter( col("message_info.route") == program_route )

        goldLakeDAO.writeTableTo(df_one_route, f"{normalize(program_route)}_hl7_validation_report_gold")


# COMMAND ----------

df1.writeStream.trigger(availableNow=True).option("mergeSchema", "true") \
    .option("checkpointLocation", globalLakeConfig.getCheckpointLocation("hl7_validation_report_gold")) \
    .foreachBatch( transformAndSendToRoute ).start()

# COMMAND ----------

# MAGIC %md
# MAGIC ### End
