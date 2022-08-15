// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Structured Streaming with Azure EventHubs 
// MAGIC 
// MAGIC ## Learning Objectives
// MAGIC By the end of this lesson, you should be able to:
// MAGIC * Establish a connection with Event Hubs in Spark
// MAGIC * Subscribe to and configure an Event Hubs stream
// MAGIC * Parse JSON records from Event Hubs
// MAGIC 
// MAGIC ## Library Requirements
// MAGIC 
// MAGIC The Maven library with coordinate `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.17`
// MAGIC 
// MAGIC ## Resources
// MAGIC - [Docs for Azure Event Hubs connector](https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/streaming-event-hubs)
// MAGIC - [Documentation on how to install Maven libraries](https://docs.azuredatabricks.net/user-guide/libraries.html#maven-or-spark-package)
// MAGIC - [Spark-EventHub debugging FAQ](https://github.com/Azure/azure-event-hubs-spark/blob/master/FAQ.md)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our classroom and set up a local streaming file read that we'll be writing to Event Hubs.

// COMMAND ----------

// MAGIC %run ./Includes/Streaming-Demo-Setup

// COMMAND ----------

// MAGIC %md
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Azure Event Hubs</h2>
// MAGIC 
// MAGIC Microsoft Azure Event Hubs is a fully managed, real-time data ingestion service.
// MAGIC You can stream millions of events per second from any source to build dynamic data pipelines and immediately respond to business challenges.
// MAGIC It integrates seamlessly with a host of other Azure services.
// MAGIC 
// MAGIC Event Hubs can be used in a variety of applications such as
// MAGIC * Anomaly detection (fraud/outliers)
// MAGIC * Application logging
// MAGIC * Analytics pipelines, such as clickstreams
// MAGIC * Archiving data
// MAGIC * Transaction processing
// MAGIC * User telemetry processing
// MAGIC * Device telemetry streaming
// MAGIC * <b>Live dashboarding</b>

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Define Connection Strings and Create Configuration Object
// MAGIC 
// MAGIC This cell uses a connection string to create a simple `EventHubsConf` object, which will be used to connect.
// MAGIC 
// MAGIC Note that the code below uses DB Utils secrets to load in the Event Hub connection string previously loaded into Azure Key Vault.
// MAGIC 
// MAGIC To run this notebook, you'll need to configure Event Hubs and provide the relavent information in the following format:
// MAGIC ```
// MAGIC Endpoint=sb://<event_hubs_namespace>.servicebus.windows.net/;SharedAccessKeyName=<key_name>;SharedAccessKey=<signing_key>=;EntityPath=<event_hubs_instance>
// MAGIC ```

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC import org.apache.spark.eventhubs.{EventHubsConf, EventPosition}
// MAGIC 
// MAGIC val connectionString = dbutils.secrets.get(scope="demo", key="ehConnectionString")
// MAGIC 
// MAGIC val ehWriteConf = EventHubsConf(connectionString)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Write Stream to Event Hub to Produce Stream
// MAGIC 
// MAGIC Below, we configure a streaming write to Event Hubs. Refer to the docs for additional ways to [write data to Event Hubs](https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/structured-streaming-eventhubs-integration.md#writing-data-to-eventhubs).

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.streaming.Trigger.ProcessingTime
// MAGIC 
// MAGIC val checkpointPath = userhome + "/event-hub/write-checkpoint"
// MAGIC dbutils.fs.rm(checkpointPath,true)
// MAGIC 
// MAGIC activityStreamDF
// MAGIC   .writeStream
// MAGIC   .format("eventhubs")
// MAGIC   .outputMode("update")
// MAGIC   .options(ehWriteConf.toMap)
// MAGIC   .trigger(ProcessingTime("25 seconds"))
// MAGIC   .option("checkpointLocation", checkpointPath)
// MAGIC   .start()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Event Hubs Configuration
// MAGIC 
// MAGIC Above, a simple `EventHubsConf` object is used to write data. There are [numerous additional options for configuration](https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/structured-streaming-eventhubs-integration.md#eventhubsconf). Below, we specify an `EventPosition` ([docs](https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/streaming-event-hubs#eventposition)) and limit our throughput by setting `MaxEventsPerTrigger`.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC val eventHubsConf = EventHubsConf(connectionString)
// MAGIC   .setStartingPosition(EventPosition.fromStartOfStream)
// MAGIC   .setMaxEventsPerTrigger(10)

// COMMAND ----------

// MAGIC %md
// MAGIC ### READ Stream using EventHub
// MAGIC 
// MAGIC The `readStream` method is a <b>transformation</b> that outputs a DataFrame with specific schema specified by `.schema()`. 

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)
// MAGIC 
// MAGIC val eventStreamDF = spark.readStream
// MAGIC   .format("eventhubs")
// MAGIC   .options(eventHubsConf.toMap)
// MAGIC   .load()
// MAGIC 
// MAGIC eventStreamDF.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Most of the fields in this response are metadata describing the state of the Event Hubs stream. We are specifically interested in the `body` field, which contains our JSON payload.
// MAGIC 
// MAGIC Noting that it's encoded as binary, as we select it, we'll cast it to a string.

// COMMAND ----------

// MAGIC %scala
// MAGIC val bodyDF = eventStreamDF.select('body.cast("STRING"))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Each line of the streaming data becomes a row in the DataFrame once an <b>action</b> such as `writeStream` is invoked.
// MAGIC 
// MAGIC Notice that nothing happens until you engage an action, i.e. a `display()` or `writeStream`.

// COMMAND ----------

// MAGIC %scala
// MAGIC display(bodyDF, streamName= "bodyDF")

// COMMAND ----------

// MAGIC %md
// MAGIC While we can see our JSON data now that it's cast to string type, we can't directly manipulate it.
// MAGIC 
// MAGIC Before proceeding, stop this stream. We'll continue building up transformations against this streaming DataFrame, and a new action will trigger an additional stream.

// COMMAND ----------

// MAGIC %scala
// MAGIC for (s <- spark.streams.active if s.name == "bodyDF") s.stop()

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## <img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Parse the JSON payload
// MAGIC 
// MAGIC The EventHub acts as a sort of "firehose" (or asynchronous buffer) and displays raw data in the JSON format.
// MAGIC 
// MAGIC If desired, we could save this as raw bytes or strings and parse these records further downstream in our processing.
// MAGIC 
// MAGIC Here, we'll directly parse our data so we can interact with the fields.
// MAGIC 
// MAGIC The first step is to define the schema for the JSON payload.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Both time fields are encoded as `LongType` here because of non-standard formatting.

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType, DoubleType}
// MAGIC 
// MAGIC lazy val schema = StructType(List(
// MAGIC   StructField("Arrival_Time", LongType),
// MAGIC   StructField("Creation_Time", LongType),
// MAGIC   StructField("Device", StringType),
// MAGIC   StructField("Index", LongType),
// MAGIC   StructField("Model", StringType),
// MAGIC   StructField("User", StringType),
// MAGIC   StructField("gt", StringType),
// MAGIC   StructField("x", DoubleType),
// MAGIC   StructField("y", DoubleType),
// MAGIC   StructField("z", DoubleType),
// MAGIC   StructField("geolocation", StructType(List(
// MAGIC     StructField("PostalCode", StringType),
// MAGIC     StructField("StateProvince", StringType),
// MAGIC     StructField("city", StringType),
// MAGIC     StructField("country", StringType)))),
// MAGIC   StructField("id", StringType)))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Parse the data
// MAGIC 
// MAGIC Next we can use the function `from_json` to parse out the full message with the schema specified above.
// MAGIC 
// MAGIC When parsing a value from JSON, we end up with a single column containing a complex object.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC import org.apache.spark.sql.functions.from_json
// MAGIC 
// MAGIC val parsedEventsDF = bodyDF.select(
// MAGIC   from_json('body, schema).alias("json"))
// MAGIC 
// MAGIC parsedEventsDF.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC Note that we can further parse this to flatten the schema entirely and properly cast our time fields.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC import org.apache.spark.sql.functions.{from_unixtime, col}
// MAGIC 
// MAGIC val flatSchemaDF = parsedEventsDF
// MAGIC   .select(from_unixtime(col("json.Arrival_Time")/1000).alias("Arrival_Time").cast("timestamp"),
// MAGIC           (col("json.Creation_Time")/1E9).alias("Creation_Time").cast("timestamp"),
// MAGIC           col("json.Device").alias("Device"),
// MAGIC           col("json.Index").alias("Index"),
// MAGIC           col("json.Model").alias("Model"),
// MAGIC           col("json.User").alias("User"),
// MAGIC           col("json.gt").alias("gt"),
// MAGIC           col("json.x").alias("x"),
// MAGIC           col("json.y").alias("y"),
// MAGIC           col("json.z").alias("z"),
// MAGIC           col("json.id").alias("id"),
// MAGIC           col("json.geolocation.country").alias("country"),
// MAGIC           col("json.geolocation.city").alias("city"),
// MAGIC           col("json.geolocation.PostalCode").alias("PostalCode"),
// MAGIC           col("json.geolocation.StateProvince").alias("StateProvince"))

// COMMAND ----------

// MAGIC %md
// MAGIC This flat schema provides us the ability to view each nested field as a column.

// COMMAND ----------

// MAGIC %scala
// MAGIC display(flatSchemaDF)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Stop all active streams

// COMMAND ----------

// MAGIC %scala
// MAGIC for (s <- spark.streams.active)
// MAGIC   s.stop

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>