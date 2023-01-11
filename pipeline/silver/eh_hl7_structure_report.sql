-- Databricks notebook source
-- MAGIC %md
-- MAGIC Updated - 12/30/2022

-- COMMAND ----------

USE SCHEMA ocio_dex_dev

-- COMMAND ----------

CREATE OR REPLACE VIEW v_hl7_structure_ok_bronze AS 
SELECT message_uuid, --message_hash,
      metadata.provenance.file_path as file_path, 
      metadata.processes[1].process_name as process_name, metadata.processes[1].status as process_status,
      date(metadata.processes[1].start_processing_time) as process_start_time,  
      --metadata.processes[1].end_processing_time as process_end_time, 
      report.status as report_status, 
      report.entries.content[1].path as report_path, report.entries.content[1].line as lineNum,
      CASE WHEN report.entries.content[1].category IS NULL THEN 'VALID MESSAGE' ELSE report.entries.content[1].category END AS category,
      CASE WHEN report.entries.content[1].classification IS NULL THEN 'VALID MESSAGE' ELSE report.entries.content[1].classification END AS classification,
   --   report.entries.content[1].category as category, report.entries.content[1].classification as classification, 
      report.entries.content[1].description as description, 
      errorCount, warningCount
      --,structureReport.process_name, structureReport.report
  FROM hl7_structure_ok_bronze;

-- COMMAND ----------

CREATE OR REPLACE VIEW v_hl7_structure_err_bronze AS 
SELECT message_uuid, --message_hash,
      metadata.provenance.file_path as file_path, 
      metadata.processes[1].process_name as process_name, metadata.processes[1].status as process_status,
      date(metadata.processes[1].start_processing_time) as process_start_time,
      --metadata.processes[1].end_processing_time as process_end_time, 
      CASE WHEN report.status IS NOT NULL THEN 'STRUCTURE ERROR' ELSE 'OTHERS' END as report_status,
      report.entries.content[1].path as report_path, report.entries.content[1].line as line,
      report.entries.content[1].category as category, report.entries.content[1].classification as classification, 
      report.entries.content[1].description as description, 
      errorCount, warningCount 
      --,structureReport.process_name, structureReport.report
  FROM hl7_structure_err_bronze;

-- COMMAND ----------

CREATE OR REPLACE VIEW v_hl7_structure_err_bronze AS 
SELECT  message_uuid, --message_hash, 
        file_path, 
        process_name, process_status, process_start_time,
        report_status,
        report_content.path, report_content.line as line,
        CASE WHEN report_content.category IS NULL THEN 'VALID MESSAGE' ELSE report_content.category END AS category,
        CASE WHEN report_content.classification  IS NULL THEN 'VALID MESSAGE' ELSE report_content.classification END AS classification,
        report_content.description as description, 
        errorCount, warningCount 
FROM (
    SELECT message_uuid,--  message_hash, 
          metadata.provenance.file_path as file_path, 
          metadata.processes[1].process_name as process_name, metadata.processes[1].status as process_status,
          date(metadata.processes[1].start_processing_time) as process_start_time,
          report.status as report_status,
          explode (report.entries.content) as report_content,
          errorCount, warningCount 
          --,structureReport.process_name, structureReport.report
      FROM hl7_structure_err_bronze
  );

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE OR REPLACE VIEW v_hl7_structure_report AS
-- MAGIC SELECT * FROM v_hl7_structure_ok_bronze
-- MAGIC UNION
-- MAGIC SELECT * FROM v_hl7_structure_err_bronze;

-- COMMAND ----------

-- MAGIC %sql 
-- MAGIC SELECT * FROM v_hl7_structure_report
-- MAGIC      -- WHERE process_status <> 'VALID_MESSAGE'

-- COMMAND ----------

-- MAGIC %sql 
-- MAGIC SELECT message_uuid,  process_name as Service, process_status, classification as Classification, category as Category,
-- MAGIC         report_path as FieldName, lineNum as Line, description as Description
-- MAGIC           FROM v_hl7_structure_report
-- MAGIC        --    WHERE process_status <> 'VALID_MESSAGE'

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT process_start_time AS process_start_time , count(*)
-- MAGIC   FROM v_hl7_structure_report
-- MAGIC   GROUP BY process_start_time order by 1

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT process_start_time as process_start_time,  report_status as report_status, count(*) 
-- MAGIC       FROM v_hl7_structure_report
-- MAGIC         GROUP BY process_start_time, report_status order by 1

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT report_path, COUNT(*)  
-- MAGIC       FROM v_hl7_structure_report
-- MAGIC         GROUP BY report_path;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT report_status, COUNT(*) AS error_cnt 
-- MAGIC   FROM v_hl7_structure_report
-- MAGIC           GROUP BY report_status;

-- COMMAND ----------

SELECT Category, COUNT(*) as error_category 
    FROM v_hl7_structure_report
            GROUP BY category;

-- COMMAND ----------

SELECT Classification, COUNT(*) 
     FROM v_hl7_structure_report
        GROUP BY classification;

-- COMMAND ----------


