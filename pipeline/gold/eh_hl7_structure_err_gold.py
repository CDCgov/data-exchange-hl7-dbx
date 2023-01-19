# Databricks notebook source
# MAGIC %sql
# MAGIC select * from ocio_dex_dev.hl7_structure_err_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC   select distinct route, message_uuid, count(*) over (partition by route) as routeCnt from ocio_dex_dev.hl7_structure_err_silver
# MAGIC   
# MAGIC  

# COMMAND ----------

from pyspark.sql import functions
df = spark.sql("select distinct route from ocio_dex_dev.hl7_Routes")

'''df = df.withColumn("route", functions.regexp_replace('route',r'[.]',"_"))'''
display(df)

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql import functions


routeList = df.collect()

df1 =  spark.readStream.format("delta").table("ocio_dex_dev.hl7_structure_err_silver")

for row in routeList:
    '''display(str(row['route']))'''
    route_check = str(row['route'])
    
 
    source_db = "ocio_dex_dev"
    target_tbl_name = "hl7_structure_err_"+ route_check.replace('.','_') + "_gold"
    target_schema_name = source_db + "." + target_tbl_name
    chkpoint_loc = "abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/events/" + target_tbl_name + "/_checkpoint" 
    df2 = df1.filter(col("route") == route_check)
    df2.writeStream.partitionBy("route").format("delta").outputMode("append").option("checkpointLocation", chkpoint_loc).toTable(target_schema_name)
     



