# Databricks notebook source
# MAGIC %md
# MAGIC ### Imports 

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Input and Output Tables

# COMMAND ----------

input_table = "ocio_dex_dev.hl7_mmg_sql_ok_silver"

output_database = "ocio_dex_dev"
output_table_suffix = "_hl7_mmg_sql_ok_TEMP_gold"

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

dfdev1 = df1.withColumn( "event_timestamp", col("metadata_provenance.event_timestamp").cast("timestamp") )

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dev Only, TODO: take out above part

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations - Singles ( mmg_sql_model_singles )

# COMMAND ----------

df_singles = df1.withColumn( "singles_keys", map_keys("mmg_sql_model_singles") ) \
                .drop("mmg_sql_model_tables")
        
display( df_singles )

# COMMAND ----------

from functools import reduce

data_itr_df_singles = df_singles.rdd.toLocalIterator()

# looping thorough each row of the dataframe
for row in data_itr_df_singles:
    
    row_message_uuid = row["message_uuid"]
    # working through each row
    print("working on (start) message_uuid: -> " + row_message_uuid)
    
    cols_singles = row["singles_keys"]
    df_one_row = df_singles.filter( col("message_uuid") == row_message_uuid )
    
    # create specific df from this row with each single in a column
    df_one_row_singles_1 = (reduce(
        lambda red_df, col_name: red_df.withColumn( col_name, red_df["mmg_sql_model_singles"][col_name] ),
        cols_singles,
        df_one_row
    ))
    
    # drop no longer needed columns
    df_one_row_singles_2 = df_one_row_singles_1.drop("mmg_sql_model_singles", "singles_keys")
    
    ######################################################################################
    # TODO: df_one_row_singles_2 write append to program table
    ######################################################################################
    program_route = "." + row["message_info"]["route"]
    output_location_full = output_database + program_route + output_table_suffix
    print(output_location_full)
#     df_one_row_singles_2.write.mode('append').saveAsTable( output_location_full )
    
    # working through each row, done this row
    print("working on (done) message_uuid: -> " + row_message_uuid)

# COMMAND ----------

display( df_one_row_singles_2 )

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

# schema_gold = ArrayType(MapType( StringType(), StringType()) )
                       

data_itr_df_tables = df_tables.rdd.toLocalIterator()

# looping thorough each row of the dataframe
for row in data_itr_df_tables:
   
    row_message_uuid = row["message_uuid"]
    # working through each row
    print("working on (start) message_uuid: -> " + row_message_uuid)
    
    cols_tables = row["tables_keys"]
    df_one_row_tables = df_tables.filter( col("message_uuid") == row_message_uuid )
    program_route = "." + row["message_info"]["route"]
    
    # create specific df from this row, one column per table
    df_one_row_tables_1 = (reduce(
        lambda red_df, col_name: red_df.withColumn( col_name, red_df["mmg_sql_model_tables"][col_name] ),
        cols_tables,
        df_one_row_tables
    ))
    
    # drop no longer needed columns
    df_one_row_tables_2 = df_one_row_tables_1.drop( "tables_keys", "mmg_sql_model_tables")
    
    # for every column (table)
    for col_table in cols_tables:
         print("working on table (start): -> " + col_table)
        
         # explode column data into rows
         # no longer selecting message_info for tables
         df_one_row_one_exploded_table = df_one_row_tables_2.select("message_uuid", col_table) \
                                .withColumn( col_table, explode(col_table) ) \
                                .withColumn( "table_keys", map_keys(col_table))

         # does this table have incoming data in this message or is null
         if not df_one_row_one_exploded_table.isEmpty():

             # grab first table_keys, they are all same for this exploded df since this df is all one repeat table
             repeat_cols = df_one_row_one_exploded_table.select("table_keys").first()[0]

             # create specific df from this row 
             repeat_table = (reduce(
                lambda red_df, col_name: red_df.withColumn( col_name, red_df[col_table][col_name] ),
                repeat_cols,
                df_one_row_one_exploded_table
             )).drop(col_table, "table_keys") # drop the map and the kyes only keep data
             # print( repeat_table )
             ######################################################################################
             # TODO: repeat_table write append to program table
             ######################################################################################
             output_table_location_full = output_database + program_route + output_table_suffix + "_" + col_table
             print(output_table_location_full)
#              repeat_table.write.mode('append').saveAsTable( output_table_location_full )
            
         print("working on table (done): -> " + col_table)
    
    # working through each row, done this row
    print("working on (done) message_uuid: -> " + row_message_uuid)

# COMMAND ----------

# MAGIC %md
# MAGIC ### End
