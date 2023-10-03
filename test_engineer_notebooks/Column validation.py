# Databricks notebook source
# MAGIC %md
# MAGIC #PURPOSE
# MAGIC ### script created to help testers in scenarios where a column has been added to 1 or more tables and the content of that column needs to be validated as well.

# COMMAND ----------

from pyspark.sql.types import *


def test_column_eval(db: str,column_name: str,file_name: list or str ,table_name: str = ''):
    exist_list = []
    not_exist_list = []

    if type(file_name) == str:
        file_name = [file_name]

    df = spark.sql(f"show tables in {db}")
    table_list = df.select("tableName").collect()
  
    if table_name:
        table_list = [x for x in table_list if table_name in x[0]]
        if not table_list:
            return print("table declared doesn't exist, please check spelling")


    for tableName in table_list:
        col_df = spark.sql(f"show columns in {db}.{tableName[0]}")
        exists_check = col_df.filter(col_df.col_name == f'{column_name}').collect()

        if exists_check:
            if '_eh_raw' in tableName[0]:

                for f_param in file_name:
                    
                    raw_col = spark.sql(f"select {column_name} from {db}.{tableName[0]} where body like '%{f_param}%'").collect()
                    
                    if raw_col != []:

                        exist_list.append([tableName[0],raw_col[0],f_param])
                    else:
                        exist_list.append([tableName[0],None,f_param])
                        
            elif '_silver' in tableName[0] or '_bronze' in tableName[0]:
                for f_param in file_name:  

                    col = spark.sql(f"select {column_name} from {db}.{tableName[0]} where provenance.ext_original_file_name = '{f_param}'").collect()
                    
                    if col != []:
                        exist_list.append([tableName[0],col[0],f_param])
                    else:
                        exist_list.append([tableName[0],None,f_param])
        
        else:
            not_exist_list.append([tableName[0],'not in table'])
    if not_exist_list:
        try:
            not_exist_df = spark.createDataFrame(not_exist_list,['table_name',column_name]) 
        except Exception as e:
            if "schema" in str(e) or "infer" in str(e):
                print(f'please contact dev, schema infer error likely caused by completely empty column {column_name}')
            else:
                print(e)
            return 
    else:
        not_exist_df = None

    if exist_list:

        try:
            exists_df = spark.createDataFrame(exist_list,['table_name',column_name,'file_name'])
        except Exception as e:
            if "schema" in str(e) or "infer" in str(e):
                print(f'please contact dev, schema infer error likely caused by completely empty column {column_name}')
            else:
                print(e)
            return 
            
    else:
        exists_df = None
    
    return [exists_df,not_exist_df]    


        

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### RUN INFORMATION
# MAGIC cluster: TST = DEX_PERF_Cluster_AD_ACLs_Disabled, DEV = DEX_DEV_Cluster_AD_ACLs_Disabled
# MAGIC
# MAGIC db: tst environment = "ocio_dex_perf", dev environment = 'ocio_dex_dev", data type = Str
# MAGIC
# MAGIC column_name: whatever the name of the column you're validating, data type = Str
# MAGIC
# MAGIC file_name: whatever the file names are of the new files that you have just sent through the pipeline to test, data type = Str or List
# MAGIC
# MAGIC

# COMMAND ----------

db = "ocio_dex_perf"
column_name = 'lake_metadata'
file_name = ["postman-urw2-3a4e6ca8-4021-4895-838a-7025d4420339.txt"]

t = test_column_eval(db,column_name,file_name)

if t:
    if t[0]:
        display(t[0])
    if t[1]:
        display(t[1])

