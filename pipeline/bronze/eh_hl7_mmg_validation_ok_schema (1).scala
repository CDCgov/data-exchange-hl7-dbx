// Databricks notebook source
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
val issueTypeSchema = new StructType()
                                  .add("category", StringType, true)
                                  .add("type", StringType, true)
                                  .add("fieldName", StringType, true)
                                  .add("hl7Path", StringType, true)
                                  .add("lineNumber", StringType, true)
                                  .add("errorMessage", StringType, true)
                                  .add("message", StringType, true)

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

// val getErrsCount = udf(( r: Row ) => {  
//     // report: List[ Map[String, String] ]
//     // reading values from row by specifying column names, can use index also
//     val report = r.getAs[ List[ Map[String, String] ] ]("report")
  
//     //size( report.filter( el => el.category === "WARNING") ) )
//   42
// })

val df4 = df3.withColumn("mmgReport", explode($"processes") ).filter( $"mmgReport.process_name" === "MMG-VALIDATOR").select("message_uuid", "message_hash", "metadata", "mmgReport")
  .withColumn("report", from_json($"mmgReport.report", new ArrayType(issueTypeSchema, true)) )

// .withColumn("errsCount", getErrsCount($"report"))
//   .withColumn("report_json", from_json($"report", new ArrayType(issueTypeSchema, true)) )
display( df4 )

// COMMAND ----------

//df4.createOrReplaceTempView( "ocio_ede_dev.tbl_hl7_structure_err_raw")
//df4.write.mode("overwrite").saveAsTable("ocio_ede_dev.tbl_hl7_mmg_ok_raw")

// COMMAND ----------

//df4.write.format("delta").mode("overwrite").saveAsTable("ocio_ede_dev.tbl_hl7_mmg_ok_raw_d")
/// Create after Dataframe DF5 after explode
//df4.write.format("delta").mode("overwrite").saveAsTable(target_schema_name)

// COMMAND ----------

val issuesDF = df4.withColumn("exploded", explode($"report"))
//display(issuesDF.select("message_uuid", "exploded.*")) -- Orgininal 
//display(issuesDF.select("message_uuid","message_hash", "mmgReport.process_name","mmgReport.status", "issuesAndErrsCount", "exploded.*"))

val df5 = issuesDF.select("message_uuid","message_hash", "mmgReport.process_name","mmgReport.status",  "exploded.*")
display (df5)
//display(issuesDF.select("message_uuid","message_hash", "metadata.provenance.file_path", "exploded.*"))

// COMMAND ----------

//df5.write.format("delta").mode("overwrite").saveAsTable("ocio_ede_dev.tbl_hl7_mmg_ok_raw_new_d")
//df5.write.format("delta").mode("overwrite").saveAsTable(target_schema_name)
//println(target_schema_name)

println(target_schema_name)
df5.writeStream.format("delta").outputMode("append").option("checkpointLocation", chkpoint_loc).toTable(target_schema_name)

// COMMAND ----------

// MAGIC %sql
// MAGIC --SELECT * FROM ocio_ede_dev.tbl_hl7_mmg_ok_raw_new_d;
// MAGIC SELECT COUNT(*) FROM ocio_dex_dev.hl7_mmg_validation_ok_bronze;

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT message_uuid, PROCESS_NAME as Service, status as Classification, 
// MAGIC         Category, hl7path as FiledName, lineNumber as Line, message as Description
// MAGIC     FROM ocio_dex_dev.hl7_mmg_validation_ok_bronze;

// COMMAND ----------


