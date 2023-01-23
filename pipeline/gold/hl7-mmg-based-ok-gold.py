# Databricks notebook source
# MAGIC %md
# MAGIC ### Imports 

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Input and Output Tables

# COMMAND ----------

input_table = "ocio_dex_dev.hl7_mmg_based_ok_silver"

output_database = "ocio_dex_dev"
output_table_suffix = "hl7_mmg_based_ok_gold"


# COMMAND ----------

# MAGIC %md
# MAGIC ### Schemas Needed

# COMMAND ----------

# MAGIC %run ../common/schemas

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Input Table

# COMMAND ----------

df1 = spark.readStream.format("delta").table( input_table )

#display( df1 )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations - MMG Based

# COMMAND ----------

df2 = df1.withColumn( "mmg_based_model_map_keys", map_keys("mmg_based_model_map") )


# COMMAND ----------

def printToFile(message):
    import datetime
    with open("./output-log.txt", "a") as f:
        f.write(f"{datetime.datetime.now()} - {message}\n")

def normalize(name):
    return name.replace(".", "_").replace(" ", "_").replace("'", "")
    
def transformAndSendToRoute(batchDF, batchId):
    routes_row_list = batchDF.select("message_info.route").distinct().collect() 
    routes_list = [x.route for x in routes_row_list]
    from functools import reduce
    for program_route in routes_list:
        # working through each batch of route
        printToFile("working on (start) route: -> " + program_route)
        df_one_route = batchDF.filter( col("message_info.route") == program_route )

        # this batch of messages they all have the same mmg, so same keys just need one (first)
        cols_needed = df_one_route.select("mmg_based_model_map_keys").first()[0]

        # create specific df from this batch adding columns for each of mmg based model entry
        df_one_batch_model1 = (reduce(
            lambda red_df, col_name: red_df.withColumn( normalize(col_name), red_df["mmg_based_model_map"][col_name] ),
            cols_needed,
            df_one_route
        ))
        
       # printToFile(df_one_batch_model1.columns)
        # drop no longer needed columns
        df_one_batch_model2 = df_one_batch_model1.drop("mmg_based_model_map", "mmg_based_model_map_keys")

        printToFile(f"records affected: {df_one_batch_model2.count()}")

        output_location_full = f"{output_database}.{normalize(program_route)}_{output_table_suffix}"
        printToFile(output_location_full)
        chkpoint_loc = f"abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/events/{output_location_full}/_checkpoint" 
        df_one_batch_model2.write.mode('append').option("checkpointLocation", chkpoint_loc).saveAsTable( output_location_full )
        # working through each batch of route
        printToFile("working on (done) route: -> " + program_route)



# COMMAND ----------

df2.writeStream.foreachBatch( transformAndSendToRoute ).start()

# COMMAND ----------

# MAGIC %md
# MAGIC ### End
