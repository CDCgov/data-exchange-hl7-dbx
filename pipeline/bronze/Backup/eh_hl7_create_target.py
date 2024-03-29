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
# display( df_source )

# COMMAND ----------

from pyspark.sql.types import *


# COMMAND ----------

processSchema = StructType([ StructField("process_name", StringType(), True), \
   StructField("process_version", StringType(), True), \
   StructField("status", StringType(), True), \
   StructField("start_processing_time", StringType(), True), \
   StructField("end_processing_time", StringType(), True), \
   StructField("report", StringType(), True) ])

schema = StructType([ StructField("content", StringType(), True), \
    StructField("message_uuid", StringType(), True), \
    StructField("metadata", StructType(
            [ StructField("provenance", 
                          StructType(
                              [ StructField("file_path", StringType(), True), \
                                StructField("file_timestamp", StringType(), True), \
                                StructField("file_size", LongType(), True), \
                                StructField("message_hash", StringType(), True), \
                                StructField("single_or_batch", StringType(), True) ]), True), \
              StructField("processes", ArrayType(processSchema, True), True) ]), True), \

    StructField("summary", StructType([ StructField("current_status", StringType(), True), \
         StructField("problem", StructType([ StructField("process_name", StringType(), True), \
                                              StructField("exception_class", StringType(), True), \
                                              StructField("stacktrace", StringType(), True), \
                                              StructField("error_message", StringType(), True), \
                                              StructField("should_retry", BooleanType(), True), \
                                              StructField("retry_count", IntegerType(), True), \
                                              StructField("max_retries", IntegerType(), True) ]), True) ]), True) ])

# COMMAND ----------

from pyspark.sql.functions import *
df1 = df_source.withColumn("bodyJson", from_json(col("body"), schema))
df_body = df1.select("bodyJson.*")
display(df_body)

# COMMAND ----------

df3 = df_body.withColumn("processes", df_body.metadata.processes).withColumn("provenance", df_body.metadata.provenance).withColumn("message_hash", df_body.metadata.provenance.message_hash)
display(df3)

# COMMAND ----------

df4 = df3.withColumn("mmgReport", explode(df3.processes)).filter( f"mmgReport.process_name == '{process_name}'").withColumn("process_version", col("mmgReport.process_version")).withColumn("process_status", col("mmgReport.status")).withColumn("process_start_time", col("mmgReport.start_processing_time")).withColumn("process_end_time", col("mmgReport.end_processing_time")).withColumn("report", col("mmgReport.report"))
display( df4 )

# COMMAND ----------

# Creating a Target table in the Database.
target_schema_name = f"{target_database}.{target_table}"

df4.writeStream.format("delta").outputMode("append").option("checkpointLocation", chkpoint_loc).toTable(target_schema_name)
