// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

// COMMAND ----------

val source_db = "ocio_dex_dev"
//val target_db = "ocio_ede_dex_dev"
val src_tbl_name = "hl7_structure_err_eh_raw"
val target_tbl_name = "hl7_structure_err_bronze"

val src_schema_name = source_db + "." + src_tbl_name
val target_schema_name = source_db + "." + target_tbl_name
val chkpoint_loc = "abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/events/" + target_tbl_name + "/_checkpoint"

val df =  spark.readStream.format("delta").table(src_schema_name)
//display(df)

// COMMAND ----------

val stackTraceSchema = new StructType().add("assertion", StringType, true).add("reasons", new ArrayType(StringType, true), true)
val issueTypeSchema = new StructType()
                             .add("line", StringType, true)
                            .add("column", StringType, true)
                            .add("path", StringType, true)
                            .add("description", StringType, true)
                            .add("category", StringType, true)
                            .add("classification", StringType, true)
                            .add("stackTrace", new ArrayType(stackTraceSchema, true) , true) // new ArrayType(processSchema, true), true )
//                             .add("metadata", new MapType , true) TODO: gives null pointer exception - 

val issueArraySchema = new ArrayType(issueTypeSchema, false)
val entriesSchema = new StructType().add("content", issueArraySchema, true).add("structure", issueArraySchema, true).add("value-set", issueArraySchema, true)

val mmgArraySchema = new ArrayType(StringType, false)
val messageInfoSchema = new StructType().add("event_code", StringType, true).add("route", StringType, true).add("mmgs", mmgArraySchema, true).add("reporting_jurisdiction", StringType, true)

// COMMAND ----------

val processSchema = new StructType() 
   .add("process_name", StringType, true)
   .add("process_version", StringType, true)
   .add("status", StringType, true)
   .add("start_processing_time", StringType, true)
   .add("end_processing_time", StringType, true)
   .add("report", new StructType()
         .add("entries", entriesSchema, true)
         .add("status", StringType, true)
         .add("error-count", new StructType()
              .add("structure", IntegerType, true)
              .add("value-set", IntegerType, true)
              .add("content", IntegerType, true)
              , true)
         .add("warning-count",  new StructType()
              .add("structure", IntegerType, true)
              .add("value-set", IntegerType, true)
              .add("content", IntegerType, true)
              , true)
        , true)

val schema =  new StructType()
    .add("content", StringType, true)
    .add("message_info", messageInfoSchema, true)
    .add("message_uuid", StringType, true)
    .add("metadata_version", StringType, true)
    .add("metadata", new StructType()    
         
         .add("provenance", new StructType()
             .add("file_path", StringType, true)
             .add("file_timestamp", StringType, true)
             .add("event_timestamp", StringType, true)
             .add("file_size", LongType, true)
             .add("message_hash", StringType, true)
             .add("message_index", StringType, true)
             .add("ext_original_file_name", StringType, true)
             .add("ext_system_provider", StringType, true)
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

val df4 = df3.withColumn("structureReport", explode($"processes") ).filter( $"structureReport.process_name" === "STRUCTURE-VALIDATOR").select("message_uuid",  "metadata_version","message_info","summary","metadata.provenance","metadata.processes", "structureReport")
  .withColumn("report", $"structureReport.report")
  .withColumn("process_name", $"structureReport.process_name")
  .withColumn("process_version", $"structureReport.process_version")
  .withColumn("validation_status", $"structureReport.status")
  .withColumn("process_start_time", $"structureReport.start_processing_time")
  .withColumn("process_end_time", $"structureReport.end_processing_time")
  .withColumn("error_count", $"report.error-count.structure" + $"report.error-count.value-set" + $"report.error-count.content" )
  .withColumn("warning_count", $"report.warning-count.structure" + $"report.warning-count.value-set" + $"report.warning-count.content" )
 
val df5 = df4.drop("structureReport")

display( df5 )

// COMMAND ----------

// Writing to Bronze table
//println(target_schema_name)
df5.writeStream.format("delta").outputMode("append")  //.trigger(Trigger.AvailableNow())
.option("checkpointLocation", chkpoint_loc).toTable(target_schema_name)

// COMMAND ----------

/*
val df_Metadata = df3.select("message_uuid","metadata.provenance.file_path", "metadata.provenance.file_size", "metadata.provenance.message_hash","metadata.provenance.message_index","metadata.provenance.single_or_batch","metadata.provenance.event_timestamp","summary.current_status", "summary.problem.process_name","summary.problem.error_message","summary.problem.should_retry","summary.problem.retry_count","summary.problem.max_retries")
display(df_Metadata)
*/
