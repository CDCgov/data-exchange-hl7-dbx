// Databricks notebook source
// MAGIC %md
// MAGIC Updated - 1/23/23
// MAGIC BY: Ramanbir (swy4)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

val source_db = "ocio_dex_dev"
val target_db = "ocio_ede_dex_dev"
val src_tbl_name = "hl7_mmg_validation_err_eh_raw"
val target_tbl_name = "hl7_mmg_validation_err_bronze"

val src_schema_name = source_db + "." + src_tbl_name
val target_schema_name = source_db + "." + target_tbl_name

val chkpoint_loc = "abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/events/" + target_tbl_name + "/_checkpoint"
val df =  spark.readStream.format("delta").option("ignoreDeletes", "true").table(src_schema_name ) 
//display( df )

// COMMAND ----------

val issueTypeSchema = new StructType()
                          .add("classification", StringType, true)
                          .add("category", StringType, true)
                          .add("fieldName", StringType, true)
                          .add("path", StringType, true)
                          .add("line", StringType, true)
                          .add("errorMessage", StringType, true)
                          .add("description", StringType, true)

val mmgArraySchema = new ArrayType(StringType, false)

// COMMAND ----------

val processSchema = new StructType() 
   .add("process_name", StringType, true)
   .add("process_version", StringType, true)
   .add("start_processing_time", StringType, true)
   .add("end_processing_time", StringType, true)
   .add("status", StringType, true)
   .add("report" , StringType, true)

val mmgReportSchema = new StructType() 
        .add("entries",  new ArrayType(issueTypeSchema, true), true )
        .add("error-count", IntegerType, true)
        .add("warning-count",  IntegerType, true)
        .add("status", StringType, true)

val schema_metadata_provenance = new StructType().add("event_id", StringType, true)
             .add("event_timestamp", StringType, true)
             .add("file_uuid", StringType, true)
             .add("file_path", StringType, true)
             .add("file_timestamp", StringType, true)
             .add("file_size", LongType, true)
             .add("single_or_batch", StringType, true)
             .add("message_hash", StringType, true)
             .add("ext_system_provider", StringType, true)
             .add("ext_original_file_name", StringType, true)
             .add("message_index", StringType, true)

val messageInfoSchema = new StructType().add("event_code", StringType, true)
           .add("route", StringType, true)
           .add("mmgs", mmgArraySchema, true)
           .add("reporting_jurisdiction", StringType, true)

val schema1 =  new StructType()
//    .add("content", StringType, true)
    .add("message_uuid", StringType, true)
    .add("metadata_version", StringType, true)
    .add("message_info", messageInfoSchema, true)
    .add("metadata", new StructType()
         .add("provenance", schema_metadata_provenance, true)
          .add("processes", new ArrayType(processSchema, true), true )
        ,true)

val schema =  new StructType()
//    .add("content", StringType, true)
    .add("message_uuid", StringType, true)
//    .add("message_hash", StringType, true)
    .add("metadata_version", StringType, true)
    .add("message_info", messageInfoSchema, true)
    .add("metadata", new StructType()
         .add("provenance", schema_metadata_provenance, true)
         .add("processes", new ArrayType(processSchema, true), true )
        ,true)

    .add("summary", new StructType()
         .add("current_status", StringType, true)
         .add("problem", new StructType()
              .add("process_name", StringType, true)
              .add("exception_class", StringType, true)
              .add("stacktrace", BooleanType, true)
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
//display(df3)
val df4 = df3.withColumn("processReport", explode($"processes") ).filter( $"processReport.process_name" === "MMG-VALIDATOR")
display(df4)


// COMMAND ----------

//val df_new = df_rep.select("processReport.report").alias("mmg_report")
val df_mmgreport = df4.withColumn("report", from_json(col("processReport.report"),mmgReportSchema))
display(df_mmgreport)

// COMMAND ----------

//Selecting fields to Create a Delta table
val df5 = df_mmgreport.select("message_uuid","metadata_version","message_info", "summary","metadata.provenance", "metadata.processes",
                             "processReport.process_name",  "processReport.process_version", "processReport.start_processing_time", 
                             "processReport.end_processing_time", "report", "report.error-count",  "report.warning-count",
                              "report.status"
                             ) 
display(df5)

// COMMAND ----------

// MAGIC %sql
// MAGIC --DROP Table ocio_dex_dev.hl7_mmg_validation_err_bronze

// COMMAND ----------

// Creating a Target Bronze table in the Database.
println(target_schema_name)
df5.writeStream.format("delta").outputMode("append").option("checkpointLocation", chkpoint_loc).toTable(target_schema_name)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM  ocio_dex_dev.hl7_mmg_validation_err_bronze;

// COMMAND ----------

// MAGIC %sql 
// MAGIC DESC ocio_dex_dev.hl7_mmg_validation_err_bronze

// COMMAND ----------


