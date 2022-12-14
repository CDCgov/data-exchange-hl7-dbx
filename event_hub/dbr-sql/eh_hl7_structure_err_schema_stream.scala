// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

val source_db = "ocio_ede_dev"
val target_db = "ocio_ede_dex_dev"
val src_tbl_name = "tbl_hl7_structure_err"
val target_tbl_name = "tbl_hl7_structure_err_bronze"

val src_schema_name = source_db + "." + src_tbl_name
val target_schema_name = source_db + "." + target_tbl_name
//val target_schema_name = target_db + "." + target_tbl_name

//val hl7StructureErrTableName = "ocio_ede_dev.tbl_hl7_structure_err"
//val df =  spark.read.format("delta").table(hl7StructureErrTableName) // .repartition(320) // The ideal number of partitions = total number of cores X 4. 
//spark.readStream.format("eventhubs").options(**ehConf).load()
val df =  spark.readStream.format("delta").table(src_schema_name)
display(df)

// COMMAND ----------

// MAGIC %sql
// MAGIC Select count(*) from ocio_ede_dev.tbl_hl7_structure_err

// COMMAND ----------

/*
  {
    "line": 72,
    "column": 5,
    "path": "OBX[69]-1[1]",
    "description": "CN-020 -  PATIENT_RESULT.ORDER_OBSERVATION.OBSERVATION.OBX-1 (Set ID - OBX) shall be valued sequentially starting with the value '1'. ",
    "category": "Constraint Failure",
    "classification": "Error",
    "stackTrace": [
        {
            "assertion": "SetId(1[1].1[1]) # Context: Group OBSERVATION (OBSERVATION)",
            "reasons": [
                "[72, 5] Expected 69, Found nn"
            ]
        }
    ],
    "metaData": {}
}
*/
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
val entriesSchema = new StructType().add("content", issueArraySchema, true).add("structure", issueArraySchema, true).add("value_set", issueArraySchema, true)

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
              .add("value_set", IntegerType, true)
              .add("content", IntegerType, true)
              , true)
         .add("warning-count",  new StructType()
              .add("structure", IntegerType, true)
              .add("value_set", IntegerType, true)
              .add("content", IntegerType, true)
              , true)
        , true)

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

val df4 = df3.withColumn("structureReport", explode($"processes") ).filter( $"structureReport.process_name" === "STRUCTURE-VALIDATOR").select("message_uuid", "message_hash", "metadata", "structureReport")
  .withColumn("report", $"structureReport.report")
  .withColumn("errCount", $"report.error-count.structure" +  $"report.error-count.value_set" +  $"report.error-count.content" )
display( df4 )

// COMMAND ----------

//df4.createOrReplaceTempView( "ocio_ede_dev.tbl_hl7_structure_err_raw")
//df4.write.mode("overwrite").saveAsTable("ocio_ede_dev.tbl_hl7_structure_err_raw")

// COMMAND ----------

//df4.write.format("delta").mode("overwrite").saveAsTable("ocio_ede_dev.tbl_hl7_structure_err_raw_d")
//target_schema_name="ocio_ede_dev.tbl_hl7_structure_err_bronze"
////df4.write.format("delta").mode("overwrite").saveAsTable(target_schema_name)

// COMMAND ----------

// MAGIC %sql
// MAGIC --DROP TABLE ocio_ede_dev.tbl_hl7_structure_err_bronze_stream

// COMMAND ----------

// MAGIC %fs 
// MAGIC ls  /tmp/delta/events/tbl_hl7_structure_err_bronze

// COMMAND ----------

//df4.write.format("delta").mode("overwrite").saveAsTable("ocio_ede_dev.tbl_hl7_structure_err_raw_d")
//target_schema_name="ocio_ede_dev.tbl_hl7_structure_err_bronze"
////df4.write.format("delta").mode("overwrite").saveAsTable(target_schema_name)

///df.writeStream.format("delta").outputMode("append").option("checkpointLocation", chkpoint_loc).toTable(schema_name)
val target_schema_name="ocio_ede_dev.tbl_hl7_structure_err_bronze_stream"
val chkpoint_loc = "/tmp/delta/events/tbl_hl7_structure_err_bronze/_checkpoints/"
println(target_schema_name)
df4.writeStream.format("delta").outputMode("append").option("checkpointLocation", chkpoint_loc).toTable(target_schema_name)

// COMMAND ----------

// MAGIC %sql
// MAGIC Select count(*) FROM ocio_ede_dev.tbl_hl7_structure_err_bronze_stream;

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE VIEW ocio_ede_dev.v_hl7_structure_err_bronze AS 
// MAGIC SELECT  message_uuid, message_hash,      --metadata.provenance.file_path as file_path, 
// MAGIC         process_name, process_status, process_start_time, process_end_time, 
// MAGIC         report_status,
// MAGIC         report_content.path, report_content.line as line,
// MAGIC         CASE WHEN report_content.category IS NULL THEN 'VALID MESSAGE' ELSE report_content.category END AS category,
// MAGIC         CASE WHEN report_content.classification  IS NULL THEN 'VALID MESSAGE' ELSE report_content.classification END AS classification,
// MAGIC         report_content.description as description, errCount
// MAGIC FROM (
// MAGIC     SELECT message_uuid, message_hash,      --metadata.provenance.file_path as file_path, 
// MAGIC           metadata.processes[1].process_name as process_name, metadata.processes[1].status as process_status,
// MAGIC           metadata.processes[1].start_processing_time as process_start_time,  metadata.processes[1].end_processing_time as process_end_time, 
// MAGIC           report.status as report_status,
// MAGIC           explode (report.entries.content) as report_content,
// MAGIC           errCount
// MAGIC           --,structureReport.process_name, structureReport.report
// MAGIC       FROM  ocio_ede_dev.tbl_hl7_structure_err_bronze_stream
// MAGIC   ) 
// MAGIC  -- where message_uuid="49ac6b1d-f29c-42e8-8961-1bafa5ae012a";

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT message_uuid, message_hash,      --metadata.provenance.file_path as file_path, 
// MAGIC       metadata.processes[1].process_name as process_name, metadata.processes[1].status as process_status,
// MAGIC       metadata.processes[1].start_processing_time as process_start_time,  metadata.processes[1].end_processing_time as process_end_time, 
// MAGIC       report.status as report_status,
// MAGIC       report.entries.content[1].path as report_path, report.entries.content[1].line as line,
// MAGIC       report.entries.content[1].category as category, report.entries.content[1].classification as classification, 
// MAGIC       report.entries.content[1].description as description, errCount
// MAGIC       --,structureReport.process_name, structureReport.report
// MAGIC   FROM ocio_ede_dev.tbl_hl7_structure_err_bronze_stream; 

// COMMAND ----------

// MAGIC %sql
// MAGIC select message_uuid, report_content.path, report_content.line from (
// MAGIC Select message_uuid, explode(report.entries.content) as report_content FROM ocio_ede_dev.tbl_hl7_structure_err_bronze_stream
// MAGIC   where message_uuid = '896b2726-8a8c-4556-9770-0afb45687862' );
