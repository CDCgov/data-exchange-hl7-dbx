# Databricks notebook source
# MAGIC %run ./eh_hl7_create_target $source_db="ocio_dex_dev" $source_table="hl7_mmg_based_ok_eh_raw" $target_db="ocio_dex_dev" $target_table="hl7_mmg_based_ok_bronze" $process_name="mmgBasedTransformer"
