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

df1 =  lakeDAO.readStreamFrom("hl7_redacted_ok_bronze")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop and Rename Columns

# COMMAND ----------

df2 = df1.drop( "processes", "status", "process_name", "process_version", "start_processing_time", "end_processing_time") \
        .withColumnRenamed("report", "redacted_report_string") 

#display( df2 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations

# COMMAND ----------

from pyspark.sql import functions as F

df3 = df2.withColumn( "report_arr", from_json( col("redacted_report_string"), schema_Redactor_Report) ) \
         .drop("redacted_report_string")

df4 = df3.withColumn('issue', F.explode_outer('report_arr.entries'))

df5 = df4.select('message_uuid', 'message_info', 'summary','provenance','config','issue.path','issue.rule','issue.lineNumber')

#display( df5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Output Table

# COMMAND ----------

lakeDAO.writeStreamTo(df5, "hl7_redacted_ok_silver" )

