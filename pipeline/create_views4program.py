# Databricks notebook source
# MAGIC %md 
# MAGIC ##Get list of tables  from both databases & Identify the one missing in target DB

# COMMAND ----------

# MAGIC %run ./eventhub2DeltaLake/eh_config

# COMMAND ----------

src_db = database_config.database
print(src_db)

# COMMAND ----------

src_db = database_config.database
#trg_db = gold_output_database("ocio_dex_prog_dev")
trg_prog_db = "ocio_dex_prog_dev"

#df_dex = spark.sql(f"show tables in ocio_dex_dev like '*_gold'")
df_dex = spark.sql(f"show tables in {src_db} like '*_gold_*'")
df_prog = spark.sql(f"show tables in {trg_prog_db} like '*_gold_*'")

## Identifying new Views to be created 
tbl_df = df_dex.select('tableName').subtract(df_prog.select('tableName'))
display(tbl_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Create Views in Target DB 

# COMMAND ----------

for vw in tbl_df.collect():
  #create a dataframe with list of Views from the database to be created
    df = spark.sql(f"CREATE OR REPLACE VIEW {trg_prog_db}.{vw.tableName} AS SELECT * FROM {src_db}.{vw.tableName} ;")
    display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Drop Views FROM Target DB which are not in Source DB

# COMMAND ----------

## Drop Views if not in Target DB
drop_tbl_df = df_prog.select('tableName').subtract(df_dex.select('tableName'))
display(drop_tbl_df)

# COMMAND ----------

for vw in drop_tbl_df.collect():
  #create a dataframe with list of Views from the database to be created
    df = spark.sql(f"DROP VIEW {trg_prog_db}.{vw.tableName} ;")
    display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --Drop TABLE ocio_dex_dev.lyme_disease_hl7_mmg_sql_ok_gold_antibiotics_treatment_repeating_group;
# MAGIC --Drop TABLE ocio_dex_dev.lyme_disease_hl7_mmg_sql_ok_gold_clinical_manifestations_repeating_group;
# MAGIC --Drop TABLE ocio_dex_dev.lyme_disease_hl7_mmg_sql_ok_gold_epidemiology_laboratory_repeating_group;
# MAGIC --Drop TABLE ocio_dex_dev.lyme_disease_hl7_mmg_sql_ok_gold_industry_and_occupation_repeating_group;
# MAGIC --Drop TABLE ocio_dex_dev.tbrd_hl7_mmg_sql_ok_gold_antibiotics_treatment_repeating_group;

# COMMAND ----------


