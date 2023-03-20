# Databricks notebook source
dbutils.widgets.dropdown("eventhub_namespace", "tf-eventhub-namespace-dev", ["tf-eventhub-namespace-dev"])

#
#dbutils.widgets.dropdown("scope_name", "dbs-scope-DEX", ["dbs-scope-DEX"])
dbutils.widgets.dropdown("scope_name", "DBS-SCOPE-DEX-DEV", ["DBS-SCOPE-DEX-DEV"])
dbutils.widgets.dropdown("database", "ocio_dex_dev", ["ocio_dex_dev"])
dbutils.widgets.dropdown("database_checkpoint_prefix", "abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/checkpoints", ["abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/checkpoints"])
dbutils.widgets.dropdown("database_folder", "abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta", ["abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta"])

#
####### this can be used if final gold moves to Edav, etc..
dbutils.widgets.dropdown("gold_output_database", "ocio_dex_prog_dev", ["ocio_dex_prog_dev"])
dbutils.widgets.dropdown("gold_output_database_checkpoint_prefix", "abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/checkpoints", ["abfss://ocio-dex-db-dev@ocioededatalakedbr.dfs.core.windows.net/delta/checkpoints"])

# COMMAND ----------

import os
# eventhub_namespace =  dbutils.widgets.get("eventhub_namespace")
# database =  dbutils.widgets.get("database")
# database_checkpoint_prefix = dbutils.widgets.get("database_checkpoint_prefix")
# database_folder = dbutils.widgets.get("database_folder")

# scope_name= dbutils.widgets.get("scope_name")
# gold_output_database =  dbutils.widgets.get("gold_output_database")
# gold_output_database_checkpoint_prefix = dbutils.widgets.get("gold_output_database_checkpoint_prefix")

dbutils.jobs.taskValues.set(key = "eventhub_namespace", value = os.getenv("eventhub_namespace"))

