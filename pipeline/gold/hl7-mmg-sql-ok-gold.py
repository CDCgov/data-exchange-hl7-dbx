# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook setting 

# COMMAND ----------

TOPIC = "hl7_mmg_sql_ok"
STAGE_IN = "silver"
STAGE_OUT = "gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports 

# COMMAND ----------

from pyspark.sql.functions import *

from functools import reduce

# COMMAND ----------

# MAGIC %run ../common/common_fns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Input and Output Tables

# COMMAND ----------

lake_util = LakeUtil( TableConfig(database_config, TOPIC, STAGE_IN, STAGE_OUT) )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Schemas Needed

# COMMAND ----------

# MAGIC %run ../common/schemas

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Stream Input Table

# COMMAND ----------

df1 = lake_util.read_stream_from_table()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function Write Single Table

# COMMAND ----------

def write_single_table(df_singles, batch_routes_list):

    # looping thorough each row of the dataframe
    for route in batch_routes_list:

        # working through each route
        printToFile(TOPIC, "working on (start) route: -> " + route)

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
        route_normalized = normalize(route)
        
        printToFile(TOPIC, f"records affected: {df_one_batch_singles_2.count()}")
        #printToFile(TOPIC, lake_util.get_for_print_gold_database_config( route_normalized ) )
        lake_util.write_gold_to_table(df_one_batch_singles_2, route_normalized)


        # working through each row, done this row
        printToFile(TOPIC, "working on (done) route: -> " + route)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Function Write Tables for Repeats

# COMMAND ----------

def write_repeat_tables(df_tables, batch_routes_list):

    # looping thorough each batch of route messages received
    for route in batch_routes_list:

        # working through each row
        printToFile(TOPIC, f"working on repeat tables (start) route: ->  + {route}")

        df_one_batch_tables = df_tables.filter( col("message_info.route") == route )
        program_route = normalize(route)

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
             printToFile(TOPIC, "working on table (start): -> " + col_table)

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

                 ######################################################################################
                 # TODO: repeat_table write append to program table
                 ######################################################################################
                 printToFile(TOPIC, f"records affected: {repeat_table.count()}")
                 col_table_norm = normalize(col_table)
                 # printToFile(TOPIC, lake_util.get_for_print_gold_database_repeat_config( program_route, col_table_norm ) )
                 lake_util.write_gold_repeat_to_table(repeat_table, program_route, col_table_norm)


             printToFile(TOPIC, "working on table (done): -> " + col_table)

        # working through each row, done this row
        printToFile(TOPIC, "working on repeat tables (done) route: -> " + route)

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

#spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")

# COMMAND ----------

#df1.writeStream.trigger(availableNow=True).foreachBatch(transform_send).start()
#.option("checkpointLocation", f"{database_folder}/checkpoints/hl7_mmg_sql_ok_silver2gold_checkpoint") \

df1.writeStream.trigger(availableNow=True).option("mergeSchema", "true") \
     .option("checkpointLocation", f"{globalDexEnv.database_checkpoint_prefix}/checkpoints/hl7_mmg_sql_ok_silver2gold_checkpoint") \
     .foreachBatch( transform_send ).start()

# COMMAND ----------

# MAGIC %md
# MAGIC ### End
