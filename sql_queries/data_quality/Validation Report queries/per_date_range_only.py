# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ####Instructions
# MAGIC
# MAGIC select options from the dropdown widgets.
# MAGIC
# MAGIC You will first, select the Database then the associated table and enter the interval in terms of days from now to display the report pertaining to that period.

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

dbutils.widgets.text('days_lookback','1')
days_ = dbutils.widgets.get('days_lookback')

# COMMAND ----------

df = spark.sql(f"SELECT * FROM {tb} WHERE provenance.ext_original_file_timestamp between date_sub(NOW(),{days_}) and now()")
display(df)

# COMMAND ----------


