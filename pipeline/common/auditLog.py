# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ../common/common_fns

# COMMAND ----------

# MAGIC %run ../common/schemas

# COMMAND ----------

lakeDAO = LakeDAO(globalLakeConfig)

# COMMAND ----------

def create_audit_log_table(df):
    spark = SparkSession.builder.getOrCreate()
   
    #logs = [('ttt',"error","recdeb","test")]
    log_df = spark.createDataFrame(df,log_schema)    
    
    lakeDAO.writeTableTo(log_df, 'hl7_audit_logs')

# COMMAND ----------

#df = [('ttt',"error",'eventHubTopic',"test")]
#create_audit_log_table(df)
