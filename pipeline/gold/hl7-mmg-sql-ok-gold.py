# Databricks notebook source
# MAGIC %md
# MAGIC ### Imports 

# COMMAND ----------

from pyspark.sql.functions import *

from functools import reduce

# COMMAND ----------

# MAGIC %run ../common/common_fns

# COMMAND ----------

#TODO: move to config when available in lake config

root_folder = 'abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/' 
db_name  = "ocio_dex_dev"

lake_config = LakeConfig(root_folder, db_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function to Log (used for Dev)

# COMMAND ----------

def print_to_file(message):
    import datetime
    with open("./output-log-sql-model-gold.txt", "a") as f:
        f.write(f"{datetime.datetime.now()} - {message}\n")
        
print_to_file("#########################################################################")

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

# MAGIC %run ../common/schemas

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Stream Input Table

# COMMAND ----------


df1 = spark.readStream.format("delta").table( input_table )

# display( df1 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function Write Single Table

# COMMAND ----------

def write_single_table(df_singles, batch_routes_list):

    # looping thorough each row of the dataframe
    for route in batch_routes_list:

        # working through each route
        print_to_file("working on (start) route: -> " + route)

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
        print_to_file(output_location_full)
        df_one_batch_singles_2.write.mode('append').saveAsTable( output_location_full )

        # working through each row, done this row
        print_to_file("working on (done) route: -> " + route)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Function Write Tables for Repeats

# COMMAND ----------

def write_repeat_tables(df_tables, batch_routes_list):

    # looping thorough each batch of route messages received
    for route in batch_routes_list:

        # working through each row
        print_to_file(f"working on repeat tables (start) route: ->  + {route}")

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
             print_to_file("working on table (start): -> " + col_table)

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
                 # print_to_file( repeat_table )
                 ######################################################################################
                 # TODO: repeat_table write append to program table
                 ######################################################################################
                 output_table_location_full = output_database + program_route + output_table_suffix + "_" + col_table
                 print_to_file(output_table_location_full)
                 repeat_table.write.mode('append').saveAsTable( output_table_location_full )

             print_to_file("working on table (done): -> " + col_table)

        # working through each row, done this row
        print_to_file("working on repeat tables (done) route: -> " + route)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function Transform and Send

# COMMAND ----------

def transform_send(batch_df, batch_id):
    batch_df.persist()
    
    batch_routes_rows = batch_df.select("message_info.route").distinct().collect()
    batch_routes_list = [z.route for z in batch_routes_rows]
    
    ##############################################
    # Singles SQL Model
    ##############################################
    df_singles = batch_df.withColumn( "singles_keys", map_keys("mmg_sql_model_singles") ) \
                .drop("mmg_sql_model_tables")

    write_single_table(df_singles, batch_routes_list)
    
    ##############################################
    # Repeats Tables SQL Model
    ##############################################
    df_tables = batch_df.withColumn("tables_keys", map_keys("mmg_sql_model_tables")) \
        .drop("mmg_sql_model_singles") \
        .drop("summary", "metadata_version", "metadata_provenance")  # not propagating to tables, only message_uuid and message_info (needed for route)

    write_repeat_tables(df_tables, batch_routes_list)
    
    batch_df.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Stream

# COMMAND ----------

 df1.writeStream.foreachBatch(transform_send).start()

# COMMAND ----------

# MAGIC %md
# MAGIC ### End
