// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

val source_db = "ocio_ede_dev"
val target_db = "ocio_ede_dex_dev"
val src_tbl_name = "tbl_hl7_structure_ok"
val target_tbl_name = "tbl_hl7_structure_ok_bronze"

val src_schema_name = source_db + "." + src_tbl_name
val target_schema_name = source_db + "." + target_tbl_name
//val target_schema_name = target_db + "." + target_tbl_name

//val hl7StructureErrTableName = "ocio_ede_dev.tbl_hl7_structure_ok"
//val df =  spark.read.format("delta").table(hl7StructureErrTableName) //.repartition(320) // The ideal number of partitions = total number of cores X 4. 

//val df = spark.read.format("delta").table(src_schema_name ) // .repartition(320) // The ideal number of partitions = total number of cores X 4. 
val df =  spark.readStream.format("delta").table(src_schema_name ) // .repartition(320) // The ideal number of partitions = total number of cores X 4. 
display( df)

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
         .add("error_count", new StructType()
              .add("structure", IntegerType, true)
              .add("value_set", IntegerType, true)
              .add("content", IntegerType, true)
              , true)
         .add("warning_count",  new StructType()
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
display( df2 )

// COMMAND ----------

val df3 = df2.withColumn("processes", $"metadata.processes")
display(df3)

// COMMAND ----------

val df4 = df3.withColumn("structureReport", explode($"processes") ).filter( $"structureReport.process_name" === "STRUCTURE-VALIDATOR").select("message_uuid", "message_hash", "metadata", "structureReport")
  .withColumn("report", $"structureReport.report")
  .withColumn("errCount", $"report.error_count.structure" +  $"report.error_count.value_set" +  $"report.error_count.content" )

display(df4)

// COMMAND ----------

//df4.createOrReplaceTempView( "ocio_ede_dev.tbl_hl7_structure_err_raw")
//df4.write.mode("overwrite").saveAsTable("ocio_ede_dev.tbl_hl7_structure_ok_raw")

// COMMAND ----------

//df4.write.format("delta").mode("overwrite").saveAsTable("ocio_ede_dev.tbl_hl7_structure_ok_raw_d")
//df4.write.format("delta").mode("overwrite").saveAsTable(target_schema_name)
//println(target_schema_name)

// COMMAND ----------

///df.writeStream.format("delta").outputMode("append").option("checkpointLocation", chkpoint_loc).toTable(schema_name)
val target_schema_name="ocio_ede_dev.tbl_hl7_structure_ok_bronze_stream"
val chkpoint_loc = "/tmp/delta/events/tbl_hl7_structure_ok_bronze/_checkpoints/"
println(target_schema_name)
df4.writeStream.format("delta").outputMode("append").option("checkpointLocation", chkpoint_loc).toTable(target_schema_name)

// COMMAND ----------

// MAGIC %sql
// MAGIC Select count(*) FROM ocio_ede_dev.tbl_hl7_structure_ok_bronze_stream

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT message_uuid, message_hash, 
// MAGIC       metadata.provenance.file_path as file_path, 
// MAGIC       metadata.processes[1].process_name as process_name, metadata.processes[1].status as process_status,
// MAGIC       metadata.processes[1].start_processing_time as process_start_time,  
// MAGIC       metadata.processes[1].end_processing_time as process_end_time, 
// MAGIC       report.status as report_status, report.entries.content[1].path as report_path, report.entries.content[1].line as lineNum,
// MAGIC       CASE WHEN report.entries.content[1].category  IS NULL THEN 'VALID' ELSE report.entries.content[1].category END AS category,
// MAGIC       CASE WHEN report.entries.content[1].classification  IS NULL THEN 'VALID' ELSE report.entries.content[1].classification END AS classification,
// MAGIC    --   report.entries.content[1].category as category, report.entries.content[1].classification as classification, 
// MAGIC       report.entries.content[1].description as description, errCount
// MAGIC       --,structureReport.process_name, structureReport.report
// MAGIC   FROM ocio_ede_dev.tbl_hl7_structure_ok_bronze_stream

// COMMAND ----------


