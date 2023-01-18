# Databricks notebook source
# source_db and source_table widgets
dbutils.widgets.dropdown("source_db", "ocio_dex_dev", ["ocio_dex_dev", "ocio_ede_dex_dev"])
source_database = dbutils.widgets.get("source_db")
source_tables = spark.catalog.listTables(source_database)
s_table_names = [t.name for t in source_tables]
dbutils.widgets.dropdown("source_table", s_table_names[0], s_table_names)
source_table = dbutils.widgets.get("source_table")


# COMMAND ----------

# target_db and target_table widgets
dbutils.widgets.dropdown("target_db", "ocio_dex_dev", ["ocio_dex_dev", "ocio_ede_dex_dev"])
target_database = dbutils.widgets.get("target_db")
target_tables = spark.catalog.listTables(target_database)
t_table_names = [t.name for t in target_tables]
dbutils.widgets.combobox("target_table", t_table_names[0], t_table_names)
target_table = dbutils.widgets.get("target_table")

# COMMAND ----------

src_schema_name = f"{source_database}.{source_table}"
chkpoint_loc = f"abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/events/{target_table}/_checkpoint"

df_source =  spark.readStream.format("delta").table(src_schema_name)


# COMMAND ----------

from pyspark.sql.functions import *
# streamingCountsDF = (                 
#   df_source
#     .groupBy(
#       df_source.partition, 
#       window(df_source.enqueuedTime, "30 days"))
#     .count()
# )
spark.conf.set("spark.sql.shuffle.partitions", "2")  # keep the size of shuffles small

query = (
  df_source
    .writeStream
    .format("memory")        # memory = store in-memory table (for testing only in Spark 2.0)
    .queryName("records")     # counts = name of the in-memory table
    .outputMode("append")  # complete = all the counts should be in the table
    .start()
)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from records;

# COMMAND ----------

new_df = _sqldf
first_json_record = new_df.select("body").first()
options = {
    'allowUnquotedFieldNames':'true',
    'allowSingleQuotes':'true',
    'allowNumericLeadingZeros': 'true'}

json_schema = new_df.select(
    schema_of_json(first_json_record[0], options).alias("json_struct")
).collect()[0][0]

print(str("Extract schema in DDL format:\n") + json_schema)

