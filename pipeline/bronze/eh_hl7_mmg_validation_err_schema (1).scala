// Databricks notebook source
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

val df =  spark.readStream.format("delta").table(src_schema_name ) 
display( df )

// COMMAND ----------

/*
data class ValidationIssue(
    val category: ValidationIssueCategoryType,      // ERROR (for required fields) or WARNING
    val type: ValidationIssueType,                  // DATA_TYPE, CARDINALITY, VOCAB
    val fieldName: String,                          // mmg field Name
    val hl7Path: String,                            // HL7 path to extract value
    val lineNumber: Int,
    val errorMessage: ValidationErrorMessage,       // error message
    val message: String,                            // custom message to add value in question

) // .ValidationIssue
*/

val stackTraceSchema = new StructType().add("assertion", StringType, true).add("reasons", new ArrayType(StringType, true), true)
val issueTypeSchema = new StructType()
                          .add("classification", StringType, true)
                          .add("category", StringType, true)
                          .add("fieldName", StringType, true)
                          .add("Path", StringType, true)
                          .add("line", StringType, true)
                          .add("errorMessage", StringType, true)
                          .add("description", StringType, true)
                          .add("stackTrace", new ArrayType(stackTraceSchema, true) , true)

val issueArraySchema = new ArrayType(issueTypeSchema, false)
val entriesSchema = new StructType().add("content", issueArraySchema, true).add("structure", issueArraySchema, true).add("value_set", issueArraySchema, true)

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

//val df4 = df3.withColumn("mmgReport", explode($"processes") ).filter( $"mmgReport.process_name" === "STRUCTURE-VALIDATOR").select("message_uuid", "message_hash", "metadata", "mmgReport")
val df4 = df3.withColumn("mmgReport", explode($"processes") ).filter( $"mmgReport.process_name" === "MMG-VALIDATOR").select("message_uuid", "message_hash", "metadata", "mmgReport")
  .withColumn("report", from_json($"mmgReport.report", new ArrayType(issueTypeSchema, true)) )
  .withColumn("issuesAndErrsCount", size($"report"))
//   .withColumn("report_json", from_json($"report", new ArrayType(issueTypeSchema, true)) )
display( df4 )
//display( df4.select("message_uuid", "message_hash","metadata.provenance.file_path") )
//df4.select(col("message_hash")).show()
//display( df4.metadata.provenance.file_path )

// COMMAND ----------

//df4.createOrReplaceTempView( "ocio_ede_dev.tbl_hl7_structure_err_raw")
//df4.write.mode("overwrite").saveAsTable("ocio_ede_dev.tbl_hl7_mmg_err_raw")

// COMMAND ----------

//df4.write.format("delta").mode("overwrite").saveAsTable("ocio_ede_dev.tbl_hl7_mmg_err_raw_d")
println(target_schema_name)
df4.writeStream.format("delta").outputMode("append").option("checkpointLocation", chkpoint_loc).toTable(target_schema_name)

// COMMAND ----------

val issuesDF = df4.withColumn("exploded", explode($"report"))
//display(issuesDF.select("message_uuid", "exploded.*")) -- Orgininal 
//display(issuesDF.select("message_uuid","message_hash", "mmgReport.process_name","mmgReport.status", "issuesAndErrsCount", "exploded.*"))

val df5 = issuesDF.select("message_uuid","message_hash", "mmgReport.process_name","mmgReport.status", "mmgReport.report", "issuesAndErrsCount", "exploded.*")
display (df5)
//display(issuesDF.select("message_uuid","message_hash", "metadata.provenance.file_path", "exploded.*"))

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM ocio_dex_dev.hl7_mmg_validation_err_bronze;

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT  * FROM ocio_dex_dev.hl7_mmg_validation_err_bronze;

// COMMAND ----------

// MAGIC %sql
// MAGIC Select message_uuid, mmgReport.process_name, explode(report) as report_content 
// MAGIC   FROM ocio_dex_dev.hl7_mmg_validation_err_bronze;

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT message_uuid, message_hash,      --metadata.provenance.file_path as file_path, 
// MAGIC       metadata.processes[1].process_name as process_name, metadata.processes[1].status as process_status,
// MAGIC     --  metadata.processes[1].start_processing_time as process_start_time,  metadata.processes[1].end_processing_time as process_end_time, 
// MAGIC       mmgReport.process_name as mmg_process_name, mmgReport.status as mmg_report_status, 
// MAGIC       report[0].category as category, report[0].type as mmg_type, 
// MAGIC       report[0].fieldname as mmg_fieldname, 
// MAGIC       report[0].hl7path as hl7path, report[0].lineNumber as lineNumber, 
// MAGIC       report[0].errorMessage as mmg_errorMessage, 
// MAGIC       report[0].message as mmg_message, 
// MAGIC       issuesAndErrsCount as total_errors
// MAGIC   FROM ocio_dex_dev.hl7_mmg_validation_err_bronze;

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT  message_uuid, message_hash, process_name , process_status,
// MAGIC         process_start_time, process_end_time,
// MAGIC         mmg_process_name, mmg_report_status,
// MAGIC         CASE WHEN report_content.category IS NULL THEN 'VALID MESSAGE' ELSE report_content.category END AS category,
// MAGIC         report_content.type, report_content.fieldName  as mmg_fieldName, 
// MAGIC         report_content.hl7Path, report_content.lineNumber,
// MAGIC         report_content.errorMessage as mmg_errorMessage, report_content.message as mmg_errorDescription
// MAGIC From (
// MAGIC     SELECT message_uuid, message_hash, 
// MAGIC         metadata.processes[1].process_name as process_name, metadata.processes[1].status as process_status,
// MAGIC         metadata.processes[1].start_processing_time as process_start_time,  metadata.processes[1].end_processing_time as process_end_time, 
// MAGIC         mmgReport.process_name as mmg_process_name, mmgReport.status as mmg_report_status, 
// MAGIC         explode(report) as report_content, 
// MAGIC         issuesAndErrsCount as total_errors
// MAGIC       FROM ocio_dex_dev.hl7_mmg_validation_err_bronze );
// MAGIC   

// COMMAND ----------

val issuesDF = df4.withColumn("exploded", explode($"report"))
//display(issuesDF.select("message_uuid", "exploded.*")) -- Orgininal 
//display(issuesDF.select("message_uuid","message_hash", "mmgReport.process_name","mmgReport.status", "issuesAndErrsCount", "exploded.*"))

val df5 = issuesDF.select("message_uuid","message_hash", "mmgReport.process_name","mmgReport.status", "issuesAndErrsCount", "exploded.*")
display (df5)
//display(issuesDF.select("message_uuid","message_hash", "metadata.provenance.file_path", "exploded.*"))

// COMMAND ----------

//df4.createOrReplaceTempView( "ocio_ede_dev.tbl_hl7_structure_err_raw")
//df5.write.mode("overwrite").saveAsTable("ocio_ede_dev.tbl_hl7_mmg_err_raw_new")
df5.write.format("delta").mode("overwrite").saveAsTable(target_schema_name)
println(target_schema_name)

// COMMAND ----------

df5.write.format("delta").mode("overwrite").saveAsTable("ocio_ede_dev.tbl_hl7_mmg_err_raw_new_d")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT count(*) FROM ocio_ede_dev.tbl_hl7_mmg_err_raw_new_d;

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT message_uuid, message_hash,      --metadata.provenance.file_path as file_path, 
// MAGIC       metadata.processes[1].process_name as process_name, metadata.processes[1].status as process_status,
// MAGIC     --  metadata.processes[1].start_processing_time as process_start_time,  metadata.processes[1].end_processing_time as process_end_time, 
// MAGIC       mmgReport.process_name as mmg_process_name, mmgReport.status as mmg_report_status, 
// MAGIC       report[0].category as category, report[0].type as mmg_type, 
// MAGIC       report[0].fieldname as mmg_fieldname, 
// MAGIC       report[0].hl7path as hl7path, report[0].lineNumber as lineNumber, 
// MAGIC       report[0].errorMessage as mmg_errorMessage, 
// MAGIC       report[0].message as mmg_message, 
// MAGIC       issuesAndErrsCount as total_errors
// MAGIC  FROM ocio_ede_dev.tbl_hl7_mmg_err_raw_new_d;

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT fieldname, category, count(*) FROM ocio_ede_dev.tbl_hl7_mmg_err_raw_new_d
// MAGIC   GROUP by fieldname, category;

// COMMAND ----------

x
