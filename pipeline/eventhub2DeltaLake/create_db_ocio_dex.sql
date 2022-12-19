-- Databricks notebook source
-- MAGIC %md 
-- MAGIC Create Databricks Database in DEX subscription

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ocio_dex_dev 
    COMMENT 'This is OCIO DEX Development Schema in DEX subscrition' 
    LOCATION 'abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/';

DESCRIBE SCHEMA ocio_dex_dev;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ocio_dex_dev.pua as
SELECT * FROM 
    csv.`abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/pua.csv`;

SELECT * FROM ocio_dex_dev.pua;    

-- COMMAND ----------


