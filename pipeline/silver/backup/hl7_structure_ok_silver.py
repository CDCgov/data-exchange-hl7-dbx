# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook setting 

# COMMAND ----------

TOPIC = "hl7_structure_ok"
STAGE_IN = "bronze"
STAGE_OUT = "silver"

# COMMAND ----------

# MAGIC %run ../common/common_fns

# COMMAND ----------

lake_util = LakeUtil( TableConfig(database_config, TOPIC, STAGE_IN, STAGE_OUT) )

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.sql import functions as F
from pyspark.sql.functions import col,concat


# COMMAND ----------

# MAGIC %run ../common/schemas

# COMMAND ----------

df = lake_util.read_stream_from_table()

# COMMAND ----------

df1 = df.withColumn( "report", from_json( col("report"), schema_report))

# COMMAND ----------


df2 = df1.select('message_uuid', 'metadata_version','message_info','summary',  'provenance','report.entries.content','report.entries.structure','report.entries.value-set')

#concat 3 arrays(structure,content, valueset)
df3 = df2.withColumn("error_concat",concat(col("content"),col("structure"),col("value-set")))


df3 = df3.withColumn('error_concat', F.explode('error_concat'))


df4 = df3.select('message_uuid','metadata_version',  'message_info', 'summary', 'provenance', 'error_concat.line','error_concat.column',df3.error_concat.path.alias("field"),'error_concat.description','error_concat.category')
# display(df4)


# COMMAND ----------

lake_util.write_stream_to_table(df4)
