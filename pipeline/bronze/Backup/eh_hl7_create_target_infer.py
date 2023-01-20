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

# process name (the report to extract from processes metadata)
dbutils.widgets.dropdown("process_name", "mmgBasedTransformer", ["RECEIVER", "MMG-VALIDATOR", "mmgBasedTransformer"] )
process_name = dbutils.widgets.get("process_name")

# COMMAND ----------

src_schema_name = f"{source_database}.{source_table}"
chkpoint_loc = f"abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/events/{target_table}/_checkpoint"

df_source =  spark.readStream.format("delta").table(src_schema_name)


# COMMAND ----------

from pyspark.sql.functions import *
spark.conf.set("spark.sql.shuffle.partitions", "2")  # keep the size of shuffles small

query = (
  df_source
    .writeStream
    .format("memory")        # memory = store in-memory table (for testing only in Spark 2.0)
    .queryName("records") 
    .outputMode("append")  
    .start()
)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from records;

# COMMAND ----------

new_df = _sqldf
first_json_record = new_df.select("body").first()
print(first_json_record[0])
options = {
    'allowUnquotedFieldNames':'true',
    'allowSingleQuotes':'true',
    'allowNumericLeadingZeros': 'true',
    'ignoreNullFields': 'false'}

json_schema = new_df.select(
    schema_of_json(first_json_record[0], options).alias("json_struct")
).collect()[0][0]

print(str("Extract schema in DDL format:\n") + json_schema)


# COMMAND ----------

df1 = new_df.withColumn("bodyJson", from_json(col("body"), json_schema))
df_body = df1.select("bodyJson.*")
display(df_body)

# COMMAND ----------

df3 = df_body.withColumn("processes", df_body.metadata.processes).withColumn("provenance", df_body.metadata.provenance).withColumn("message_hash", df_body.metadata.provenance.message_hash)
display(df3)

# COMMAND ----------

df4 = df3.withColumn("mmgReport", explode(df3.processes)).filter( f"mmgReport.process_name == '{process_name}'").withColumn("process_version", col("mmgReport.process_version")).withColumn("process_status", col("mmgReport.status")).withColumn("process_start_time", col("mmgReport.start_processing_time")).withColumn("process_end_time", col("mmgReport.end_processing_time")).withColumn("report", col("mmgReport.report"))
display( df4 )

# COMMAND ----------


# Creating a Target Bronze table in the Database.
target_schema_name = f"{target_database}.{target_table}"

df4.write.format("delta").outputMode("append").option("checkpointLocation", chkpoint_loc).toTable(target_schema_name)
