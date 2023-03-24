-- Databricks notebook source
-- MAGIC %md 
-- MAGIC Create Databricks Database in DEX subscription

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ocio_edav_dex_dev 
    COMMENT 'This is OCIO DEX Development Schema in DEX subscrition' 
    LOCATION 'abfss://database@edavdevdatalakedex.dfs.core.windows.net/';

DESCRIBE SCHEMA ocio_edav_dex_dev;

-- COMMAND ----------

use database ocio_edav_dex_dev

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS pua as
SELECT * FROM 
    csv.`abfss://database@edavdevdatalakedex.dfs.core.windows.net/pua.csv`;

SELECT * FROM pua;    

-- COMMAND ----------

Drop table pua

-- COMMAND ----------


