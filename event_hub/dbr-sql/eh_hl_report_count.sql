-- Databricks notebook source
SELECT "hl7_file_dropped", Count(*) FROM ocio_ede_dev.tbl_hl7_file_dropped

-- COMMAND ----------

SELECT "hl7_recdeb_ok" ,  Count(*) FROM ocio_ede_dev.tbl_hl7_recdeb_ok;

-- COMMAND ----------

SELECT "hl7_recdeb_err" ,  Count(*) FROM ocio_ede_dev.tbl_hl7_recdeb_err;

-- COMMAND ----------

SELECT "hl7_structure_ok", count(*) FROM ocio_ede_dev.tbl_hl7_structure_ok;

-- COMMAND ----------

SELECT "hl7_structure_err", count(*) FROM ocio_ede_dev.tbl_hl7_structure_err; 

-- COMMAND ----------

SELECT "hl7_mmg_validation_ok", count(*) FROM ocio_ede_dev.tbl_hl7_mmg_validation_ok;

-- COMMAND ----------

SELECT "hl7_mmg_validation_err", count(*) FROM ocio_ede_dev.tbl_hl7_mmg_validation_err

-- COMMAND ----------

SELECT "hl7_message_processor_ok", count(*) FROM ocio_ede_dev.tbl_hl7_message_processor_ok

-- COMMAND ----------

SELECT "hl7_message_processor_err", count(*) FROM ocio_ede_dev.tbl_hl7_message_processor_err

-- COMMAND ----------

SELECT "hl7_mmg_based_ok", count(*) FROM ocio_ede_dev.tbl_hl7_mmg_based_ok

-- COMMAND ----------

SELECT "hl7_mmg_based_err", count(*) FROM ocio_ede_dev.tbl_hl7_mmg_based_err

-- COMMAND ----------

SELECT "hl7_file_dropped" , Count(*) FROM ocio_ede_dev.tbl_hl7_file_dropped
UNION
SELECT "hl7_recdeb_ok" ,  Count(*) FROM ocio_ede_dev.tbl_hl7_recdeb_ok
UNION 
SELECT "hl7_recdeb_err" ,  Count(*) FROM ocio_ede_dev.tbl_hl7_recdeb_err
UNION
SELECT "hl7_structure_ok", count(*) FROM ocio_ede_dev.tbl_hl7_structure_ok
UNION
SELECT "hl7_structure_err", count(*) FROM ocio_ede_dev.tbl_hl7_structure_err
UNION
SELECT "hl7_mmg_validation_ok", count(*) FROM ocio_ede_dev.tbl_hl7_mmg_validation_ok
UNION
SELECT "hl7_mmg_validation_err", count(*) FROM ocio_ede_dev.tbl_hl7_mmg_validation_err
UNION
--SELECT "hl7_message_processor_ok", count(*) FROM ocio_ede_dev.tbl_hl7_message_processor_ok
--UNION
--SELECT "hl7_message_processor_err", count(*) FROM ocio_ede_dev.tbl_hl7_message_processor_err
--UNION
SELECT "hl7_mmg_based_ok", count(*) FROM ocio_ede_dev.tbl_hl7_mmg_based_ok
UNION
SELECT "hl7_mmg_based_err", count(*) FROM ocio_ede_dev.tbl_hl7_mmg_based_ok



-- COMMAND ----------


