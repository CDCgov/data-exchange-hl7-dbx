# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ###Instructions
# MAGIC
# MAGIC This report shows you the count of valid and errored messages grouped by date
# MAGIC
# MAGIC Please select the Database and the corresponding table before executing the query cell.
# MAGIC
# MAGIC

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

df = spark.sql(f"""select to_date(provenance.ext_original_file_timestamp) as dt, count(case when status='MMG_VALID' or status='VALID_MESSAGE' then 1 end) as Valid_Messages, count(case when status='STRUCTURE_ERRORS' then 1 end) as Errored_Messages from  {tb} where provenance.ext_original_file_timestamp is not null GROUP BY dt""")
display(df)

# COMMAND ----------


