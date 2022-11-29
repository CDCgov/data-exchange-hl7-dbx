# Databricks notebook source
saved = spark.sql("select * from ocio_ede_dev.tbl_hl7_message_processor_ok sort by enqueuedTime desc")
display(saved)

# COMMAND ----------



# COMMAND ----------

saved = spark.sql("select body:metadata:processes[3]:report from ocio_ede_dev.tbl_hl7_message_processor_ok ")
display(saved)

# COMMAND ----------

import json
from pyspark.sql import functions as F

from pyspark.sql.functions import get_json_object
saved  = spark.sql("select body:message_uuid, body:metadata:processes[3]:report from ocio_ede_dev.tbl_hl7_message_processor_ok sort by enqueuedTime desc")

bodycollect = saved.collect()

all_keys = []
for entry in bodycollect:
    dictionary = json.loads(entry[1])
    
    for key in dictionary.keys():
        if not key in all_keys:
            all_keys.append(str(key))

'/**display(len(all_keys))'
df = saved.select('message_uuid',F.json_tuple('report', *all_keys)).toDF('message_uuid',*all_keys)

'/** df2 = df.filter("message_uuid == 0332acec-4f68-4842-9bbd-49287d72d4ba")'
row = df.count()
 
print(f'Number of Rows:  + {row}')

df2 = df.where("message_uuid == '84b68965-badd-40c0-84ab-cbd1addbe5e3'")
display(df)


# COMMAND ----------


