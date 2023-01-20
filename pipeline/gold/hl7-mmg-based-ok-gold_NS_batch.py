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
output_table_suffix = "hl7_mmg_based_ok_TEMP_gold"

#TODO:
output_checkpoint = ""

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schemas Needed

# COMMAND ----------

# MAGIC %run ../common/schemas

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Input Table

# COMMAND ----------

#TODO: change to streaming
# df1 = spark.readStream.format("delta").table( input_table )

df1 = spark.read.format("delta").table( input_table )

display( df1 )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Dev Only, TODO: take out below part

# COMMAND ----------

spark.sql("select current_date(), current_timestamp()").show(truncate=False)

# COMMAND ----------

dfdev1 = df1.withColumn( "event_timestamp", col("provenance.event_timestamp").cast("timestamp") )

dfdev2 = dfdev1.orderBy( col("event_timestamp").desc() )

cutoff = "2023-01-13T16:48:59.992+0000" 

dfdev3 = dfdev2.filter( col("event_timestamp") > cutoff )

display( dfdev3 )
dfdev3.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Re-assign df1, dev only

# COMMAND ----------

df1 = dfdev3.drop("event_timestamp")

display( df1 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dev Only, TODO: take out above part

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations - MMG Based

# COMMAND ----------

df2 = df1.withColumn( "mmg_based_model_map_keys", map_keys("mmg_based_model_map") ) \
        
# display( df2 )

print( df2.count() )


# COMMAND ----------

routes_row_list = df2.select("message_info.route").distinct().collect() 

routes_list = [x.route for x in routes_row_list]

print( routes_list )

# COMMAND ----------

from functools import reduce

for program_route in routes_list:
    
    # working through each batch of route
    print("working on (start) route: -> " + program_route)
    df_one_route = df2.filter( col("message_info.route") == program_route )
    
    # this batch of messages they all have the same mmg, so same keys just need one (first)
    cols_needed = df_one_route.select("mmg_based_model_map_keys").first()[0]
    
    # create specific df from this batch adding columns for each of mmg based model entry
    df_one_batch_model1 = (reduce(
        lambda red_df, col_name: red_df.withColumn( col_name, red_df["mmg_based_model_map"][col_name] ),
        cols_needed,
        df_one_route
    ))
    
    # drop no longer needed columns
    df_one_batch_model2 = df_one_batch_model1.drop("mmg_based_model_map", "mmg_based_model_map_keys")
    
    ######################################################################################
    # TODO: df_one_batch_model2 write append to program table
    ######################################################################################

    output_location_full = f"{output_database}.{program_route}_{output_table_suffix}"
    print(output_location_full)
#     df_one_batch_model2.write.mode('append').saveAsTable( output_location_full )

    # working through each batch of route
    print("working on (done) route: -> " + program_route)
    
    

# COMMAND ----------

display( df_one_batch_model2 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### End
