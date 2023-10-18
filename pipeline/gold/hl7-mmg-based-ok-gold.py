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
# MAGIC ### Schemas Needed

# COMMAND ----------

# MAGIC %run ../common/schemas

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Input Table

# COMMAND ----------

lakeDAO = LakeDAO(globalLakeConfig)
goldLakeDAO = LakeDAO(globalGOLDLakeConfig)

df1 = lakeDAO.readStreamFrom("hl7_mmg_based_ok_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations - MMG Based

# COMMAND ----------

df2 = df1.withColumn( "mmg_based_model_map_keys", map_keys("mmg_based_model_map") )


# COMMAND ----------

def convert_complex_type(red_df, col_name):
    # red_df.withColumn( normalize(col_name), red_df["mmg_based_model_map"][col_name] )

    red_df = red_df.withColumn( normalize(col_name), red_df["mmg_based_model_map"][col_name] )

    import pyspark.sql.functions as F
    # schema = F.schema_of_json(red_df.select(normalize(col_name)).head()[0])

    # data_type = red_df.schema[normalize(col_name)]

    # print(f'Data type of {normalize(col_name)}: {data_type}')

    try:
    #if (col(col_name).startswith("[{")) or (col(col_name).startswith("{")):
        schema = F.schema_of_json(red_df.select(normalize(col_name)).head()[0])
        red_df = red_df.withColumn(col_name + "_NEW", F.from_json(normalize(col_name), schema))
    except:
        print("non JSON field")

    # if normalize(col_name) == 'message_profile_identifier':
    #     schema = F.schema_of_json(red_df.select(normalize(col_name)).head()[0])
    #     red_df = red_df.withColumn(col_name + "_NEW", F.from_json(normalize(col_name), schema))

    return red_df

# COMMAND ----------

   
def transformAndSendToRoute(batchDF, batchId):
    routes_row_list = batchDF.select("message_info.route").distinct().collect() 
    routes_list = [x.route for x in routes_row_list]
    from functools import reduce
    for program_route in routes_list:
        # working through each batch of route
        # printToFile(TOPIC, "working on (start) route: -> " + program_route)
        df_one_route = batchDF.filter( col("message_info.route") == program_route )

        # this batch of messages they all have the same mmg, so same keys just need one (first)
        cols_needed = df_one_route.select("mmg_based_model_map_keys").first()[0]

        # create specific df from this batch adding columns for each of mmg based model entry
        # df_one_batch_model1 = (reduce(
        #     # lambda red_df, col_name: red_df.withColumn( normalize(col_name), red_df["mmg_based_model_map"][col_name] ),
        #     lambda red_df, col_name: convert_complex_type(red_df.withColumn( normalize(col_name), red_df["mmg_based_model_map"][col_name] ), col_name),
        #     cols_needed,
        #     df_one_route
        # ))

        for col_name in cols_needed:
            # df_one_route = df_one_route.withColumn( normalize(col_name), df_one_route["mmg_based_model_map"][col_name] )
            df_one_route = convert_complex_type(df_one_route, col_name)

        
        # drop no longer needed columns
        df_one_batch_model2 = df_one_route.drop("mmg_based_model_map", "mmg_based_model_map_keys")
        df_one_batch_model2.show(2)

        # printToFile(TOPIC, f"records affected: {df_one_batch_model2.count()}")
        #printToFile(TOPIC, lake_util.get_for_print_gold_database_config( program_route ) )
        # lake_util.write_gold_to_table(df_one_batch_model2, program_route)
        goldLakeDAO.writeTableTo(df_one_batch_model2, f"{normalize(program_route)}_hl7_mmg_based_ok_gold")
        # working through each batch of route
        # printToFile(TOPIC, "working on (done) route: -> " + program_route)


# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")

# COMMAND ----------

#df2.writeStream.trigger(availableNow=True).foreachBatch( transformAndSendToRoute ).start()
df2.writeStream.trigger(availableNow=True).option("overwriteSchema", "true").option("mergeSchema", "true") \
    .option("checkpointLocation", globalLakeConfig.getCheckpointLocation("hl7_mmg_based_ok_silver2gold_checkpoint")) \
    .foreachBatch( transformAndSendToRoute ).start()

# COMMAND ----------

# MAGIC %md
# MAGIC ### End
