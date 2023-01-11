-- Databricks notebook source
-- MAGIC %md
-- MAGIC Updated - 12/30/2022

-- COMMAND ----------

USE SCHEMA ocio_dex_dev

-- COMMAND ----------

CREATE OR REPLACE VIEW v_hl7_mmg_validation_ok_bronze AS 
SELECT message_uuid, -- message_hash,
      metadata.provenance.file_path as file_path, 
      metadata.processes[2].process_name as process_name, metadata.processes[2].status as process_status,
      date(metadata.processes[2].start_processing_time) as process_start_time,  
      --metadata.processes[1].end_processing_time as process_end_time, 
      mmgreport.status as report_status, 
      NULL as report_path , NULL as lineNum , NULL AS Category, NULL AS Classification, NULL AS Description, 
--      mmgreport.entries.content[1].path as report_path, 
     -- report.entries.content[1].line as lineNum,
    --  CASE WHEN report.entries.content[1].category  IS NULL THEN 'VALID MESSAGE' ELSE report.entries.content[1].category END AS category,
    --  CASE WHEN report.entries.content[1].classification  IS NULL THEN 'VALID MESSAGE' ELSE report.entries.content[1].classification END AS classification,
   --   report.entries.content[1].category as category, report.entries.content[1].classification as classification, 
    --  report.entries.content[1].description as description, 
      errorCount[0] as errorCount , warningCount[0] as warningCount
  FROM hl7_mmg_validation_ok_bronze;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC Select message_uuid, errorCount  FROM v_hl7_mmg_validation_ok_bronze

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE OR REPLACE VIEW v_hl7_mmg_validation_err_bronze AS 
-- MAGIC SELECT message_uuid, -- message_hash,
-- MAGIC       metadata.provenance.file_path as file_path, 
-- MAGIC       metadata.processes[2].process_name as process_name, metadata.processes[2].status as process_status,
-- MAGIC       date(metadata.processes[2].start_processing_time) as process_start_time, 
-- MAGIC       CASE WHEN mmgreport.status IS NOT NULL THEN 'MMG Validation ERROR' ELSE 'OTHERS' END as report_status,
-- MAGIC       report.entries[0].path as report_path, report.entries[0].line as lineNum,
-- MAGIC --      CASE WHEN report.entries[0].category IS NULL THEN 'VALID MESSAGE' ELSE report.entries[0].category END AS category,
-- MAGIC --      CASE WHEN report.entries[0].classification  IS NULL THEN 'VALID MESSAGE' ELSE report.entries[0].classification END AS classification,
-- MAGIC       report.entries[0].category as category, report.entries[0].classification as classification, 
-- MAGIC       report.entries[0].description as description, 
-- MAGIC       errorCount[0] as errorCount , warningCount[0] as warningCount
-- MAGIC       --,structureReport.process_name, structureReport.report
-- MAGIC   FROM hl7_mmg_validation_err_bronze;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE OR REPLACE VIEW v_hl7_mmg_validation_err_bronze AS 
-- MAGIC SELECT  message_uuid, --message_hash, 
-- MAGIC         file_path, 
-- MAGIC         process_name, process_status, process_start_time,
-- MAGIC         report_status,
-- MAGIC         report_content.path as report_path, report_content.line as lineNum,
-- MAGIC         report_content.category, report_content.classification,
-- MAGIC --        CASE WHEN report_content.category IS NULL THEN 'VALID MESSAGE' ELSE report_content.category END AS category,
-- MAGIC --        CASE WHEN report_content.classification  IS NULL THEN 'VALID MESSAGE' ELSE report_content.classification END AS classification,
-- MAGIC         
-- MAGIC         report_content.description as description, 
-- MAGIC         errorCount, warningCount
-- MAGIC FROM (
-- MAGIC     SELECT message_uuid, message_hash, 
-- MAGIC           metadata.provenance.file_path as file_path, 
-- MAGIC           metadata.processes[2].process_name as process_name, metadata.processes[2].status as process_status,
-- MAGIC           date(metadata.processes[2].start_processing_time) as process_start_time,
-- MAGIC           mmgreport.status as report_status,
-- MAGIC           explode (report[0].entries) as report_content,
-- MAGIC          errorCount[0] as errorCount , warningCount[0] as warningCount
-- MAGIC           --,structureReport.process_name, structureReport.report
-- MAGIC       FROM hl7_mmg_validation_err_bronze
-- MAGIC       );

-- COMMAND ----------

CREATE OR REPLACE VIEW v_hl7_mmg_validation_report AS
SELECT * FROM v_hl7_mmg_validation_ok_bronze
UNION
SELECT * FROM v_hl7_mmg_validation_err_bronze;

-- COMMAND ----------

DESC v_hl7_mmg_validation_report

-- COMMAND ----------

SELECT message_uuid, process_status, process_name as Service, process_status, classification as Classification, category as Category,
        report_path as FieldName, lineNum as Line, description as Description
          FROM v_hl7_mmg_validation_report
  --              WHERE process_status <> 'MMG_VALID'

-- COMMAND ----------

SELECT process_start_time as process_start_time , count(*)
    FROM v_hl7_mmg_validation_report
    GROUP BY process_start_time order by 1

-- COMMAND ----------

SELECT  process_start_time as process_start_time,  report_status as report_status, count(*) 
  FROM v_hl7_mmg_validation_report
    GROUP BY process_start_time, report_status order by 1

-- COMMAND ----------

SELECT report_status, COUNT(*) AS error_cnt 
  FROM v_hl7_mmg_validation_report
    GROUP BY report_status;

-- COMMAND ----------

SELECT Category, COUNT(*) as error_category 
    FROM v_hl7_structure_report
            GROUP BY category;

-- COMMAND ----------

SELECT Classification, COUNT(*) 
     FROM v_hl7_structure_report
        GROUP BY classification;

-- COMMAND ----------


