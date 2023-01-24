# Databricks notebook source
# MAGIC %md
# MAGIC ### Imports 

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Input and Output Tables

# COMMAND ----------

input_table = "ocio_dex_dev.hl7_mmg_validation_err_silver"

output_database = "ocio_dex_dev"
output_table_suffix = "hl7_mmg_validation_err_gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Input Table

# COMMAND ----------

df1 = spark.readStream.format("delta").table( input_table )
display( df1 )

# COMMAND ----------

def printToFile(message):
    import datetime
    with open("./output-log.txt", "a") as f:
        f.write(f"{datetime.datetime.now()} - {message}\n")

def normalize(name):
    if name is not None:
        return name.replace(".", "_").replace(" ", "_").replace("'", "")
    else:
        return str(name)
    
def transformAndSendToRoute(batchDF, batchId):
    routes_row_list = batchDF.select("message_info.route").distinct().collect() 
    routes_list = [x.route for x in routes_row_list]
    from functools import reduce
    for program_route in routes_list:
        # working through each batch of route
        printToFile("working on (start) route: -> " + str(program_route))
        # check if route == null, then push data into none table
        if(program_route == 'None'):
            df_one_route = batchDF.filter(col("message_info.route").isNull())
        else:    
            df_one_route = batchDF.filter( col("message_info.route") == program_route )

        output_location_full = f"{output_database}.{normalize(program_route)}_{output_table_suffix}"
        printToFile(output_location_full)
        chkpoint_loc = f"abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/events/{output_location_full}/_checkpoint" 
        df_one_route.write.mode('append').option("checkpointLocation", chkpoint_loc).saveAsTable( output_location_full )
        # working through each batch of route
        printToFile("working on (done) route: -> " + str(program_route))



# COMMAND ----------

df1.writeStream.foreachBatch( transformAndSendToRoute ).start()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ocio_dex_dev.lyme_disease_hl7_mmg_validation_err_gold

# COMMAND ----------

# MAGIC %md
# MAGIC ### End
