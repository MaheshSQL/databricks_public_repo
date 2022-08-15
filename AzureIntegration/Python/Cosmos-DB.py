# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading and writing from CosmosDB
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Write data into Cosmos DB
# MAGIC * Read data from Cosmos DB
# MAGIC 
# MAGIC Cosmos DB requires some dependencies; details on configuration can be found [here](https://docs.databricks.com/data/data-sources/azure/cosmosdb-connector.html).

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Load Cosmos DB
# MAGIC 
# MAGIC Now load a small amount of data into Cosmos to demonstrate that connection

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC <span>1.</span> Enter the CosmosDB connection information into the cell below. <br>

# COMMAND ----------

PrimaryKey = dbutils.secrets.get(scope="demo", key="cosmoskey")
URI = dbutils.secrets.get(scope="demo", key="cosmosuri")
CosmosDatabase = "dbretail"
CosmosCollection = "rating"

cosmosConfig = {
  "Endpoint": URI,
  "Masterkey": PrimaryKey,
  "Database": CosmosDatabase,
  "Collection": CosmosCollection
}

# COMMAND ----------

# MAGIC %md
# MAGIC <span>2.</span> Read the input parquet file.

# COMMAND ----------

from pyspark.sql.functions import col
ratingsDF = (spark.read
  .parquet("dbfs:/mnt/training/initech/ratings/ratings.parquet/")
  .withColumn("rating", col("rating").cast("double")))
print("Num Rows: {}".format(ratingsDF.count()))

# COMMAND ----------

display(ratingsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC <span>3.</span> Write the data to Cosmos DB.

# COMMAND ----------

ratingsSampleDF = ratingsDF.sample(.0001)

(ratingsSampleDF.write
  .mode("overwrite")
  .format("com.microsoft.azure.cosmosdb.spark")
  .options(**cosmosConfig)
  .save())

# COMMAND ----------

# MAGIC %md
# MAGIC <span>4.</span> Confirm that your data is now in Cosmos DB.

# COMMAND ----------

dfCosmos = (spark.read
  .format("com.microsoft.azure.cosmosdb.spark")
  .options(**cosmosConfig)
  .load())

display(dfCosmos)


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>