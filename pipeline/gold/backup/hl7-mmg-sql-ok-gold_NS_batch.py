# Databricks notebook source
# MAGIC %md
# MAGIC ### Imports 

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ../../common/common_fns

# COMMAND ----------

#TODO: move to config when available in lake config


root_folder = 'abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/' 
db_name  = "ocio_dex_dev"

lake_config = LakeConfig(root_folder, db_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Input and Output Tables

# COMMAND ----------

input_table = "ocio_dex_dev.hl7_mmg_sql_ok_silver"

output_database = "ocio_dex_dev"
output_table_suffix = "hl7_mmg_sql_ok_gold"

output_table_name = "hl7_mmg_sql_ok_gold"
# #TODO:
# output_checkpoint = ""

output_checkpoint = lake_config.getCheckpointLocation(output_table_name)
# output_checkpoint + "_lyme..."


# COMMAND ----------

# MAGIC %md
# MAGIC ### Schemas Needed

# COMMAND ----------

# MAGIC %run ../../common/schemas

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
# MAGIC ### Routes in Batch

# COMMAND ----------

batch_routes_rows = df1.select("message_info.route").distinct().collect()

batch_routes_list = [z.route for z in batch_routes_rows]

batch_routes_list

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations - Singles ( mmg_sql_model_singles )

# COMMAND ----------

df_singles = df1.withColumn( "singles_keys", map_keys("mmg_sql_model_singles") ) \
                .drop("mmg_sql_model_tables")
        
display( df_singles )

# COMMAND ----------

from functools import reduce


# looping thorough each row of the dataframe
for route in batch_routes_list:
    
    # working through each route
    print("working on (start) route: -> " + route)
    
    df_one_batch = df_singles.filter( col("message_info.route") == route )
        
    cols_singles = df_one_batch.select("singles_keys").first()[0] 
    
    # create specific df from this batch with each single in a column
    df_one_batch_singles_1 = (reduce(
        lambda red_df, col_name: red_df.withColumn( col_name.replace(".", "_"), red_df["mmg_sql_model_singles"][col_name] ),
        cols_singles,
        df_one_batch
    ))
    
    # drop no longer needed columns
    df_one_batch_singles_2 = df_one_batch_singles_1.drop("mmg_sql_model_singles", "singles_keys")
    
    ######################################################################################
    # TODO: df_one_batch_singles_2 write append to program table
    ######################################################################################
    route_normalized = route.replace(".", "_")
    output_location_full = f"{output_database}.{route_normalized}_{output_table_suffix}"
    print(output_location_full)
    df_one_batch_singles_2.write.mode('append').saveAsTable( output_location_full )
    
    # working through each row, done this row
    print("working on (done) route: -> " + route)

# COMMAND ----------

# display( df_one_batch_singles_2 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations - Repeats ( mmg_sql_model_tables )

# COMMAND ----------

# from df1 initial df
df_tables = df1.withColumn("tables_keys", map_keys("mmg_sql_model_tables")) \
        .drop("mmg_sql_model_singles") \
        .drop("summary", "metadata_version", "metadata_provenance")  # not propagating to tables, only message_uuid and message_info (needed for route)
        
display( df_tables )

# COMMAND ----------

# display( df_tables.select("mmg_sql_model_tables.transfused_product" ) )

# COMMAND ----------

                 
# looping thorough each batch of route messages received
for route in batch_routes_list:
   
    # working through each row
    print(f"working on (start) route: ->  + {route}")
          
    df_one_batch_tables = df_tables.filter( col("message_info.route") == route )
    program_route = route.replace(".", "_")
          
    cols_tables = df_one_batch_tables.select("tables_keys").first()[0]
    
    # create specific df from this batch, one column per table of repeat
    df_one_batch_tables_1 = (reduce(
        lambda red_df, col_name: red_df.withColumn( col_name.replace(".", "_"), red_df["mmg_sql_model_tables"][col_name] ),
        cols_tables,
        df_one_batch_tables
    ))
    
    # drop no longer needed columns
    df_one_batch_tables_2 = df_one_batch_tables_1.drop( "tables_keys", "mmg_sql_model_tables")
    
    # for every column (table)
    for col_table in cols_tables:
         print("working on table (start): -> " + col_table)
        
         # explode column data into rows
         # no longer selecting message_info for tables
         df_one_batch_one_exploded_table = df_one_batch_tables_2.select("message_uuid", col_table) \
                                .withColumn( col_table, explode(col_table) ) \
                                .withColumn( "table_keys", map_keys(col_table))

         # does this table have incoming data in this message or is null
         if not df_one_batch_one_exploded_table.isEmpty():

             # grab first table_keys, they are all same for this exploded df since this df is all one repeat table
             repeat_cols = df_one_batch_one_exploded_table.select("table_keys").first()[0]

             # create specific df from this row 
             repeat_table = (reduce(
                lambda red_df, col_name: red_df.withColumn( col_name.replace(".", "_"), red_df[col_table][col_name] ),
                repeat_cols,
                df_one_batch_one_exploded_table
             )).drop(col_table, "table_keys") # drop the map and the kyes only keep data
             # print( repeat_table )
             ######################################################################################
             # TODO: repeat_table write append to program table
             ######################################################################################
             output_table_location_full = output_database + "." + program_route + "_" + output_table_suffix + "_" + col_table
             print(output_table_location_full)
             repeat_table.write.mode('append').saveAsTable( output_table_location_full )
            
         print("working on table (done): -> " + col_table)
    
    # working through each row, done this row
    print("working on (done) route: -> " + route)

# COMMAND ----------

# MAGIC %md
# MAGIC ### End
