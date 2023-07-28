# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Instructions
# MAGIC
# MAGIC select options from the dropdown widgets
# MAGIC
# MAGIC database: select an option from the databases
# MAGIC
# MAGIC days_lookback: designate the desired amount of days to look back in time
# MAGIC
# MAGIC table_name: select a table from a list of table names depending on the database
# MAGIC
# MAGIC event_code: select one or more event_code based on distinct values retrieved from the selected table
# MAGIC
# MAGIC jurisdictions: select one or more jurisdiction based on distinct values retrieved from the selected table
# MAGIC

# COMMAND ----------

db = spark.sql("show databases").collect()
db_list = [x[0] for x in db if 'edav_dex' in x[0]]

dbutils.widgets.dropdown("database",db_list[0],db_list)
db_get = dbutils.widgets.get("database")
table_df = spark.sql(f"show tables in {db_get}").select('tableName').collect()
table_list = [x[0] for x in table_df]

dbutils.widgets.dropdown('table_name',table_list[0],table_list)
table_name = dbutils.widgets.get('table_name')


event_codes = spark.sql(f"select distinct message_info.event_code from {db_get}.{table_name}").collect()
event_codes_list = [x[0] for x in event_codes]
dbutils.widgets.multiselect('event_code',event_codes_list[0],event_codes_list)
#event_code_get = dbutils.widgets.get('event_code')

jurisdiction_var = spark.sql(f"select distinct cast(message_info.reporting_jurisdiction as string) from {db_get}.{table_name}").collect()
jurisdiction_list = [x[0] for x in jurisdiction_var]
dbutils.widgets.multiselect('jurisdictions',jurisdiction_list[0],jurisdiction_list)


dbutils.widgets.text('days_lookback','1')
#days_lookback = int(dbutils.widgets.get('days_lookback'))


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC with latest 
# MAGIC as 
# MAGIC (select *
# MAGIC   ,rank() over(partition by message_info.local_record_id order by provenance.ext_original_file_timestamp desc,message_uuid) as i_seq
# MAGIC   from $database.$table_name
# MAGIC   where message_info.local_record_id is not null
# MAGIC )
# MAGIC
# MAGIC select *
# MAGIC from  latest l 
# MAGIC where l.i_seq = 1
# MAGIC and l.provenance.ext_original_file_timestamp between date_sub(date_trunc('day',NOW()),$days_lookback) and now()
# MAGIC and cast(l.message_info.event_code as int) in ($event_code)
# MAGIC and cast(l.message_info.reporting_jurisdiction as int) in ($jurisdictions)
# MAGIC
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC
# MAGIC
