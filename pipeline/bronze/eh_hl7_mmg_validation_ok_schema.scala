// Databricks notebook source
// MAGIC %md
// MAGIC Updated 12/29/2022
// MAGIC     By: Ramanbir (swy4)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

val source_db = "ocio_dex_dev"
val target_db = "ocio_ede_dex_dev"
val src_tbl_name = "hl7_mmg_validation_ok_eh_raw"
val target_tbl_name = "hl7_mmg_validation_ok_bronze"

val src_schema_name = source_db + "." + src_tbl_name
val target_schema_name = source_db + "." + target_tbl_name
val chkpoint_loc = "abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/events/" + target_tbl_name + "/_checkpoint"

val df =  spark.readStream.format("delta").table(src_schema_name ) 
//display( df )

// COMMAND ----------

val issueTypeSchema = new StructType()
                          .add("classification", StringType, true)
                          .add("category", StringType, true)
                          .add("fieldName", StringType, true)
                          .add("Path", StringType, true)
                          .add("line", StringType, true)
                          .add("errorMessage", StringType, true)
                          .add("description", StringType, true)
//                          .add("error-count", IntegerType, true)
//                          .add("warning-count", IntegerType, true)

val issueArraySchema = new ArrayType(issueTypeSchema, false)
//val entriesSchema = new StructType().add("content", issueArraySchema, true).add("structure", issueArraySchema, true).add("value_set", issueArraySchema, true)
val entriesSchema = new StructType().add("entries", issueArraySchema, true)
                         .add("error-count", IntegerType, true) 
                         .add("warning-count", IntegerType, true)                   

// COMMAND ----------

val processSchema = new StructType() 
   .add("process_name", StringType, true)
   .add("process_version", StringType, true)
   .add("status", StringType, true)

   .add("start_processing_time", StringType, true)
   .add("end_processing_time", StringType, true)

   .add("report", StringType, true)  // This will get from_json below

val schema =  new StructType()
    .add("content", StringType, true)
    .add("message_uuid", StringType, true)
    .add("message_hash", StringType, true)
    .add("metadata", new StructType()
         
         .add("provenance", new StructType()
             .add("file_path", StringType, true)
             .add("file_timestamp", StringType, true)
             .add("file_size", LongType, true)
             .add("single_or_batch", StringType, true), true)
         
        .add("processes", new ArrayType(processSchema, true), true ))

    .add("summary", new StructType()
         .add("current_status", StringType, true)
         .add("problem", new StructType()
              .add("process_name", StringType, true)
              .add("exception_class", StringType, true)
              .add("stacktrace", StringType, true)
              .add("error_message", StringType, true)
              .add("should_retry", BooleanType, true)
              .add("retry_count", IntegerType, true)
              .add("max_retries", IntegerType, true), true)
             , true)

// COMMAND ----------

val df1 = df.withColumn("bodyJson", from_json(col("body"), schema))
val df2 = df1.select("bodyJson.*")
display(df2)

// COMMAND ----------

val df3 = df2.withColumn("processes", $"metadata.processes")
display(df3)

// COMMAND ----------

val df4 = df3.withColumn("mmgReport", explode($"processes") ).filter( $"mmgReport.process_name" === "MMG-VALIDATOR").select("message_uuid", "message_hash", "metadata", "mmgReport")
    .withColumn("report", from_json($"mmgReport.report", new ArrayType(entriesSchema, true)) )
    .withColumn("errorCount", $"report.error-count" )
    .withColumn("warningCount", $"report.warning-count" )
//    .withColumn("issuesAndErrsCount", size($"report"))
display( df4 )

// COMMAND ----------

// Creating a Target Bronze table in the Database.
println(target_schema_name)
df4.writeStream.format("delta").outputMode("append").option("checkpointLocation", chkpoint_loc).toTable(target_schema_name)

// COMMAND ----------

// MAGIC %sql
// MAGIC DESC table ocio_dex_dev.hl7_mmg_validation_ok_bronze

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(*) FROM ocio_dex_dev.hl7_mmg_validation_ok_bronze
