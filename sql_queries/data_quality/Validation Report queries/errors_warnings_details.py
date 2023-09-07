# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ###Instructions
# MAGIC
# MAGIC This report shows you a straight list or errors and warnings with description for a selected table
# MAGIC
# MAGIC Please select the Database and the corresponding table before executing the query cell.

# COMMAND ----------

db = spark.sql("show databases").collect()
db_list = [x[0] for x in db if 'edav_dex' in x[0]]

dbutils.widgets.dropdown("database",db_list[0],db_list)
db_get = dbutils.widgets.get("database")

table_df = spark.sql(f"show tables in {db_get}").select('tableName').collect()
table_list = [x[0] for x in table_df if 'validation' in x[0]]
dbutils.widgets.dropdown('table_name',table_list[0],table_list)
table_name = dbutils.widgets.get('table_name')
tb = db_get+"."+table_name

# COMMAND ----------

df = spark.sql(f"SELECT DISTINCT classification, category, description FROM {tb} WHERE classification IS NOT NULL OR category IS NOT NULL")
display(df)

# COMMAND ----------


