# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook setting 

# COMMAND ----------

# MAGIC %run ../common/common_fns

# COMMAND ----------

# MAGIC %run ../common/schemas

# COMMAND ----------

lakeDAO = LakeDAO(globalLakeConfig)

df_err    = lakeDAO.readStreamFrom("hl7_mmg_validation_err_bronze")
df_ok     = lakeDAO.readStreamFrom("hl7_mmg_validation_ok_bronze")

# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql.functions import col, concat, from_json

df_result = df_ok.unionByName(df_err, allowMissingColumns=True)

df2 = df_result.withColumn('issue', F.explode_outer((from_json('report', mmgReportSchema)).entries))

df3 = df2.select('message_uuid', 'metadata_version','message_info','status','summary', 'provenance', 'issue.category','issue.line', 
                 df2.issue.path.alias("column"),df2.issue.fieldName.alias("field"),'issue.classification','issue.description' )


# COMMAND ----------

lakeDAO.writeStreamTo(df3, "hl7_mmg_validation_silver")

