# Databricks notebook source
# MAGIC %sql
# MAGIC select * from ocio_dex_dev.hl7_structure_err_silver

# COMMAND ----------

from pyspark.sql import functions
df_route = spark.sql("select distinct route from ocio_dex_dev.hl7_Routes")

'''df = df.withColumn("route", functions.regexp_replace('route',r'[.]',"_"))'''
display(df_route)

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql import functions

# convert routes df to list
routeList = df_route.collect()

df1 =  spark.readStream.format("delta").table("ocio_dex_dev.hl7_structure_err_silver")

# looping through each row of the routes dataframe
for row in routeList:
    '''display(str(row['route']))'''
    route_check = str(row['route'])    
 
    source_db = "ocio_dex_dev"
    # constructing table names for each route
    target_tbl_name = route_check.replace('.','_') + "_hl7_structure_err_"+"_gold"
    
    target_schema_name = source_db + "." + target_tbl_name
    
    chkpoint_loc = "abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/events/" + target_tbl_name + "/_checkpoint"
    
    if(route_check == 'None'):
        df2 = df1.filter(col("message_info.route").isNull())       
    else:
        df2 = df1.filter(col("message_info.route") == route_check)
    
    df2.writeStream.format("delta").outputMode("append").option("checkpointLocation", chkpoint_loc).toTable(target_schema_name)
     



