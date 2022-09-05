# Databricks notebook source
# MAGIC %md <a href='$../Azure Integrations Start Here'>Home</a>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://jokdemoresourcessa.blob.core.windows.net/images/azure_newDB_color_transp.png" alt="Azure Databricks" width="600"><br/>
# MAGIC <img src="https://jokdemoresourcessa.blob.core.windows.net/images/StreamingGraphic.png" alt="Streaming" width="600">
# MAGIC <img src="https://jokdemoresourcessa.blob.core.windows.net/images/weather.jpg" alt="Streaming" width="600">
# MAGIC # Event Hub Streaming

# COMMAND ----------

# Azure Event Hub connection parameters
ehns = 'jokdemoresourceseh'
eventhub = 'weather'
sa_pol = 'bidirect'
sa_key = 'Tx2n89okSrIdU8DeU1SQCo79KKAvhnu0s85Mtykj1fw='

# COMMAND ----------

# Setup connection to Azure Event Hub
from pyspark.sql.types import StructField, StructType, StringType, Row
import json

connectionString = 'Endpoint=sb://'+ehns+'.servicebus.windows.net/;SharedAccessKeyName='+sa_pol+';SharedAccessKey='+sa_key+';EntityPath='+eventhub
ehConf = {
  'eventhubs.connectionString' : connectionString
}
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)

dfWeather = spark \
  .readStream \
  .format("eventhubs") \
  .options(**ehConf) \
  .load()

# COMMAND ----------

display(dfWeather)

# COMMAND ----------

display(dfWeather.withColumn('body', dfWeather['body'].cast('string')))

# COMMAND ----------

from pyspark.sql.functions import json_tuple

display(dfWeather
        .withColumn("temperature", json_tuple(dfWeather["body"].cast("string"), "temp"))
        .withColumn("humidity", json_tuple(dfWeather["body"].cast("string"), "humidity"))
         .withColumn("light", json_tuple(dfWeather["body"].cast("string"), "lux"))  
        .drop("body", "partition", "offset", "publisher", "partitionKey", "properties", "systemProperties")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a Delta Lake table from the stream for Analysis

# COMMAND ----------

deltaWeather_df = (dfWeather
  .writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "/dbfs/tmp/weather1")
  .table("weather")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### We can query the Delta Lake table at the same time as it is being written to (by the stream)

# COMMAND ----------

# MAGIC %sql
# MAGIC select enqueuedTime, json_tuple(strbody, 'temp', 'humidity', 'rainsensor', 'dry', 'lux') as (temperature, humidity, rainres, dry, lux) from (
# MAGIC select cast(body as string) as strbody, enqueuedTime from weather) order by enqueuedTime desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select day, round(avg(temperature),1) as avtemp from (
# MAGIC select date_format(enqueuedTime, 'yyyy-MM-dd') as day, json_tuple(strbody, 'temp', 'humidity', 'rainsensor', 'dry', 'lux') as (temperature, humidity, rainres, dry, lux) from (
# MAGIC select cast(body as string) as strbody, enqueuedTime from weather)) group by day order by day desc

# COMMAND ----------


