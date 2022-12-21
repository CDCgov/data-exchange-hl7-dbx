-- Databricks notebook source
SELECT "hl7_file_dropped", Count(*) FROM ocio_dex_dev.hl7_file_dropped_eh_raw

-- COMMAND ----------

SELECT "hl7_recdeb_ok" ,  Count(*) FROM ocio_dex_dev.hl7_recdeb_ok_eh_raw;

-- COMMAND ----------

SELECT "hl7_recdeb_err" ,  Count(*) FROM ocio_dex_dev.hl7_recdeb_err_eh_raw;

-- COMMAND ----------

SELECT "hl7_structure_ok", count(*) FROM ocio_dex_dev.hl7_structure_ok_eh_raw;

-- COMMAND ----------

SELECT "hl7_structure_err", count(*) FROM ocio_dex_dev.hl7_structure_err_eh_raw; 

-- COMMAND ----------

SELECT "hl7_mmg_validation_ok", count(*) FROM ocio_dex_dev.hl7_mmg_validation_ok_eh_raw;

-- COMMAND ----------

SELECT "hl7_mmg_validation_err", count(*) FROM ocio_dex_dev.hl7_mmg_validation_err_eh_raw

-- COMMAND ----------

SELECT "hl7_mmg_based_ok", count(*) FROM ocio_dex_dev.hl7_mmg_based_ok_eh_raw

-- COMMAND ----------

SELECT "hl7_mmg_based_err", count(*) FROM ocio_dex_dev.hl7_mmg_based_err_eh_raw

-- COMMAND ----------

SELECT "hl7_file_dropped" , Count(*) FROM ocio_dex_dev.hl7_file_dropped_eh_raw
UNION
SELECT "hl7_recdeb_ok" ,  Count(*) FROM ocio_dex_dev.hl7_recdeb_ok_eh_raw
UNION 
SELECT "hl7_recdeb_err" ,  Count(*) FROM ocio_dex_dev.hl7_recdeb_err_eh_raw
UNION
SELECT "hl7_structure_ok", count(*) FROM ocio_dex_dev.hl7_structure_ok_eh_raw
UNION
SELECT "hl7_structure_err", count(*) FROM ocio_dex_dev.hl7_structure_err_eh_raw
UNION
SELECT "hl7_mmg_validation_ok", count(*) FROM ocio_dex_dev.hl7_mmg_validation_ok_eh_raw
UNION
SELECT "hl7_mmg_validation_err", count(*) FROM ocio_dex_dev.hl7_mmg_validation_err_eh_raw
UNION
--SELECT "hl7_message_processor_ok", count(*) FROM ocio_dex_dev.hl7_message_processor_ok_eh_raw
--UNION
--SELECT "hl7_message_processor_err", count(*) FROM ocio_dex_dev.hl7_message_processor_err_eh_raw
--UNION
SELECT "hl7_mmg_based_ok", count(*) FROM ocio_dex_dev.hl7_mmg_based_ok_eh_raw
UNION
SELECT "hl7_mmg_based_err", count(*) FROM ocio_dex_dev.hl7_mmg_based_ok_eh_raw



-- COMMAND ----------

DESC ocio_dex_dev.hl7_mmg_validation_ok_bronze

-- COMMAND ----------

DESC ocio_dex_dev.hl7_mmg_validation_err_bronze

-- COMMAND ----------

DESC ocio_dex_dev.hl7_structure_err_bronze

-- COMMAND ----------

DESC ocio_dex_dev.hl7_structure_ok_bronze

-- COMMAND ----------


