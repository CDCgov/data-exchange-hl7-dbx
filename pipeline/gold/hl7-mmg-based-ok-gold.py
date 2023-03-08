# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook setting 

# COMMAND ----------

TOPIC = "hl7_mmg_based_ok"
STAGE_IN = "silver"
STAGE_OUT = "gold"

# COMMAND ----------

# MAGIC %run ../common/common_fns

# COMMAND ----------

lake_util = LakeUtil( TableConfig(database_config, TOPIC, STAGE_IN, STAGE_OUT) )


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

df1 = lake_util.read_stream_from_table()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations - MMG Based

# COMMAND ----------

df2 = df1.withColumn( "mmg_based_model_map_keys", map_keys("mmg_based_model_map") )


# COMMAND ----------

   
def transformAndSendToRoute(batchDF, batchId):
    routes_row_list = batchDF.select("message_info.route").distinct().collect() 
    routes_list = [x.route for x in routes_row_list]
    from functools import reduce
    for program_route in routes_list:
        # working through each batch of route
        printToFile(TOPIC, "working on (start) route: -> " + program_route)
        df_one_route = batchDF.filter( col("message_info.route") == program_route )

        # this batch of messages they all have the same mmg, so same keys just need one (first)
        cols_needed = df_one_route.select("mmg_based_model_map_keys").first()[0]

        # create specific df from this batch adding columns for each of mmg based model entry
        df_one_batch_model1 = (reduce(
            lambda red_df, col_name: red_df.withColumn( normalize(col_name), red_df["mmg_based_model_map"][col_name] ),
            cols_needed,
            df_one_route
        ))
        
        # drop no longer needed columns
        df_one_batch_model2 = df_one_batch_model1.drop("mmg_based_model_map", "mmg_based_model_map_keys")

        printToFile(TOPIC, f"records affected: {df_one_batch_model2.count()}")
        #printToFile(TOPIC, lake_util.get_for_print_gold_database_config( program_route ) )
        lake_util.write_gold_to_table(df_one_batch_model2, program_route)

        # working through each batch of route
        printToFile(TOPIC, "working on (done) route: -> " + program_route)


# COMMAND ----------

#spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")

# COMMAND ----------

#df2.writeStream.trigger(availableNow=True).foreachBatch( transformAndSendToRoute ).start()
df2.writeStream.trigger(availableNow=True).option("mergeSchema", "true").foreachBatch( transformAndSendToRoute ).start()

# COMMAND ----------

# MAGIC %md
# MAGIC ### End
