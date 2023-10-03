# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ####Instructions
# MAGIC
# MAGIC select options from the dropdown widgets and enter the corresponding input items
# MAGIC
# MAGIC First, select the Database then the associated table and enter the interval in terms of days from now to display the report pertaining to that period.
# MAGIC You also have the ability to select the event code and the jurisdictions

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

event_codes = spark.sql(f"select distinct message_info.event_code from {db_get}.{table_name}").collect()

event_codes_list = [x[0] for x in event_codes]

dbutils.widgets.multiselect('event_code',event_codes_list[0],event_codes_list)

event_code_get = dbutils.widgets.get('event_code')



jurisdiction_var = spark.sql(f"select distinct message_info.reporting_jurisdiction from {db_get}.{table_name}").collect()

jurisdiction_list = [x[0] for x in jurisdiction_var]

dbutils.widgets.multiselect('jurisdictions',jurisdiction_list[0],jurisdiction_list)

jurisdiction_get = dbutils.widgets.get('jurisdictions')

# COMMAND ----------

dbutils.widgets.text('days_lookback','1')
days_ = dbutils.widgets.get('days_lookback')

juris = jurisdiction_get.split(',')
event = event_code_get.split(',')

# COMMAND ----------

if len(event) == 1 and len(juris) > 1 :
    tuple_juris = tuple([int(x) for x in juris])
    df = spark.sql(f"""SELECT * FROM {tb} WHERE provenance.ext_original_file_timestamp between date_sub(NOW(),{days_}) and now() and message_info.event_code ={event_code_get} and message_info.reporting_jurisdiction in {tuple_juris}""")

elif len(juris) ==1 and len(event)>1:
    tuple_event = tuple([int(y) for y in event])
    df = spark.sql(f"""SELECT * FROM {tb} WHERE provenance.ext_original_file_timestamp between date_sub(NOW(),{days_}) and now() and message_info.event_code in {tuple_event} and message_info.reporting_jurisdiction={jurisdiction_get}""")

elif len(juris) ==1 and len(event)==1:
        df = spark.sql(f"""SELECT * FROM {tb} WHERE provenance.ext_original_file_timestamp between date_sub(NOW(),{days_}) and now() and message_info.event_code ={event_code_get} and message_info.reporting_jurisdiction ={jurisdiction_get}""")
        
display(df)

# COMMAND ----------


