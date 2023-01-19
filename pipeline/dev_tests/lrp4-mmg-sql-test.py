# Databricks notebook source

mmg_sql_model = spark.sql("SELECT * FROM ocio_dex_dev.hl7_mmg_sql_ok_eh_raw")

display(mmg_sql_model)

# COMMAND ----------


df1 = spark.sql("SELECT * FROM ocio_dex_dev.hl7_mmg_sql_ok_eh_raw sort by enqueuedTime desc")
display(df1)



# COMMAND ----------



df2 = spark.sql("select body:metadata:processes[4]:report from ocio_dex_dev.hl7_mmg_sql_ok_eh_raw ")
display(df2)



# COMMAND ----------

import json
from pyspark.sql import functions as F

from pyspark.sql.functions import get_json_object
saved  = spark.sql("select body:message_uuid, body:metadata:processes[4]:report from ocio_dex_dev.hl7_mmg_sql_ok_eh_raw")

bodycollect = saved.collect()

all_keys = []
for entry in bodycollect:
    if entry[1]:
        dictionary = json.loads(entry[1])
    
    for key in dictionary.keys():
        if not key in all_keys:
            all_keys.append(str(key))

'/**display(len(all_keys))'
df = saved.select('message_uuid',F.json_tuple('report', *all_keys)).toDF('message_uuid',*all_keys)

'/** df2 = df.filter("message_uuid == b1ddfc1a-8add-40be-ad4f-99ed0f6bdf25")'
row = df.count()
 
print(f'Number of Rows:  + {row}')

display(df)


# COMMAND ----------

dff = df.where("message_uuid == 'b1ddfc1a-8add-40be-ad4f-99ed0f6bdf25'")

display(dff)

# COMMAND ----------

display(dff.select("tables"))


# COMMAND ----------


