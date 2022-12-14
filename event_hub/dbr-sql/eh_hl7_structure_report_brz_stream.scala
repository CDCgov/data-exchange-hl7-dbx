// Databricks notebook source
// MAGIC %sql
// MAGIC Select * from ocio_ede_dev.tbl_hl7_structure_err_bronze_stream

// COMMAND ----------

// MAGIC  %sql
// MAGIC CREATE OR REPLACE VIEW ocio_ede_dev.v_hl7_structure_ok_bronze_stream AS 
// MAGIC SELECT message_uuid, message_hash,
// MAGIC       metadata.provenance.file_path as file_path, 
// MAGIC       metadata.processes[1].process_name as process_name, metadata.processes[1].status as process_status,
// MAGIC       date(metadata.processes[1].start_processing_time) as process_start_time,  
// MAGIC       metadata.processes[1].end_processing_time as process_end_time, 
// MAGIC       report.status as report_status, 
// MAGIC       report.entries.content[1].path as report_path, report.entries.content[1].line as lineNum,
// MAGIC       CASE WHEN report.entries.content[1].category  IS NULL THEN 'VALID MESSAGE' ELSE report.entries.content[1].category END AS category,
// MAGIC       CASE WHEN report.entries.content[1].classification  IS NULL THEN 'VALID MESSAGE' ELSE report.entries.content[1].classification END AS classification,
// MAGIC    --   report.entries.content[1].category as category, report.entries.content[1].classification as classification, 
// MAGIC       report.entries.content[1].description as description, errCount
// MAGIC       --,structureReport.process_name, structureReport.report
// MAGIC   FROM ocio_ede_dev.tbl_hl7_structure_ok_bronze_stream;

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE VIEW ocio_ede_dev.v_hl7_structure_err_bronze_stream AS 
// MAGIC SELECT  message_uuid, message_hash, file_path, 
// MAGIC         process_name, process_status, process_start_time, process_end_time, 
// MAGIC         report_status,
// MAGIC         report_content.path, report_content.line as line,
// MAGIC         CASE WHEN report_content.category IS NULL THEN 'VALID MESSAGE' ELSE report_content.category END AS category,
// MAGIC         CASE WHEN report_content.classification  IS NULL THEN 'VALID MESSAGE' ELSE report_content.classification END AS classification,
// MAGIC         report_content.description as description, errCount
// MAGIC FROM (
// MAGIC     SELECT message_uuid, message_hash,      
// MAGIC           metadata.provenance.file_path as file_path, 
// MAGIC           metadata.processes[1].process_name as process_name, metadata.processes[1].status as process_status,
// MAGIC           metadata.processes[1].start_processing_time as process_start_time,  metadata.processes[1].end_processing_time as process_end_time, 
// MAGIC           report.status as report_status,
// MAGIC           explode (report.entries.content) as report_content,
// MAGIC           errCount
// MAGIC           --,structureReport.process_name, structureReport.report
// MAGIC       FROM  ocio_ede_dev.tbl_hl7_structure_err_bronze_stream
// MAGIC   ) 

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE VIEW ocio_ede_dev.v_hl7_structure_bronze_stream AS
// MAGIC SELECT * FROM ocio_ede_dev.v_hl7_structure_ok_bronze_stream
// MAGIC UNION
// MAGIC SELECT * FROM ocio_ede_dev.v_hl7_structure_err_bronze_stream;

// COMMAND ----------

// MAGIC %sql 
// MAGIC Select * from ocio_ede_dev.v_hl7_structure_bronze_stream
// MAGIC --  where message_uuid = "4e428948-50cb-4641-860f-e95bab235a6e"

// COMMAND ----------

// MAGIC %sql
// MAGIC Select * FROM ocio_ede_dev.v_hl7_structure_bronze_stream
// MAGIC   where process_status <> 'VALID_MESSAGE'

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT date(process_start_time) as process_start_time , count(*)
// MAGIC   FROM ocio_ede_dev.v_hl7_structure_bronze_stream
// MAGIC   GROUP BY date(process_start_time) order by 1

// COMMAND ----------

// MAGIC %sql
// MAGIC --SELECT Date(structureReport.start_processing_time) as msg_process_date,report.status as report_status, count(*) 
// MAGIC --  FROM ocio_ede_dev.tbl_hl7_structure_err_raw
// MAGIC --  GROUP BY msg_process_date, report.status ORDER BY 1
// MAGIC   
// MAGIC SELECT date(process_start_time) as process_start_time,  report_status as Classification, count(*) 
// MAGIC   FROM ocio_ede_dev.v_hl7_structure_bronze_stream
// MAGIC   GROUP BY date(process_start_time), report_status order by 1

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT report_path, COUNT(*)  FROM ocio_ede_dev.v_hl7_structure_bronze_streamv_hl7_structure_bronze
// MAGIC   GROUP BY report_path order by 1;

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT report_status, COUNT(*) AS error_cnt FROM ocio_ede_dev.v_hl7_structure_bronze_stream
// MAGIC     GROUP BY report_status;

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT category, COUNT(*) as error_category 
// MAGIC   FROM ocio_ede_dev.v_hl7_structure_bronze_stream
// MAGIC   GROUP BY category;

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT classification, COUNT(*) 
// MAGIC   FROM ocio_ede_dev.v_hl7_structure_bronze
// MAGIC     GROUP BY classification;

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM ocio_ede_dev.v_hl7_structure_bronze_stream;

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT message_uuid, PROCESS_NAME as Service, Classification, Category, report_path as FiledName, lineNum as Line, Description
// MAGIC     FROM ocio_ede_dev.v_hl7_structure_bronze_stream;

// COMMAND ----------


