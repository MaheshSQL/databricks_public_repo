# Databricks notebook source
# MAGIC %md <a href='$../Azure Integrations Start Here'>Home</a>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://gsethistorageaccount.blob.core.windows.net/images/CosmosDB.png" alt="Azure CosmosDB" width="400">
# MAGIC # Reading and Writing to CosmosDB

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://gsethistorageaccount.blob.core.windows.net/images/Jup_214407_pipp_lapl5_ap27-wavelet.jpg'>
# MAGIC <img src='https://gsethistorageaccount.blob.core.windows.net/images/Saturn_223110_pipp_lapl5_ap43-wavelet.jpg'>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configure Write parameters to Azure CosmosDB

# COMMAND ----------

# SECRETS - Demo use only - these should be stored in AKV or a Secret Scope
cosmosEndpoint = "https://gsethi-cosmosdb.documents.azure.com:443/"
cosmosKey = "7AgSLseb011NTcHhYYVwSHyq8nSUa9NWsoYigF37w43pEePiosUsUvaNB4gPhrieBlkgSjCo2cIyuxMdwUtScw=="
cosmosDatabase = "gsethicosmosdb"
cosmosContainer = "planets"

cosmosConfig = {
 "spark.cosmos.accountEndpoint" : cosmosEndpoint,
 "spark.cosmos.accountKey" : cosmosKey,
 "spark.cosmos.database" : cosmosDatabase,
 "spark.cosmos.container" : cosmosContainer,
 "spark.cosmos.write.strategy" : "ItemOverwrite"
}

# COMMAND ----------

# Use the Catalog API
spark.conf.set("spark.sql.catalog.cosmosCatalog", "com.azure.cosmos.spark.CosmosCatalog")
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountEndpoint", cosmosEndpoint)
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountKey", cosmosKey)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Container in the Cosmos Database

# COMMAND ----------

sqlCommand = "create table if not exists cosmosCatalog.{}.{} using cosmos.oltp TBLPROPERTIES(partitionKeyPath = '/type')"
spark.sql(sqlCommand.format(cosmosDatabase, cosmosContainer))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write first eight records

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, DoubleType, IntegerType

planetSchema = StructType([
  StructField("id", StringType(), True),
  StructField("name", StringType(), True),
  StructField("type", StringType(), True),
  StructField("au", DoubleType(), True),
  StructField("ediameter", DoubleType(), True)
])

planetsDF = spark.createDataFrame([Row("1", "Mercury", "terrestrial", 0.39, 0.382),Row("2", "Venus", "terrestrial", 0.72, 0.949), Row("3", "Earth", "terrestrial", 1.0, 1.0),Row("4", "Mars", "terrestrial", 1.52,0.532),Row("5", "Jupiter", "gas", 5.2, 11.209),Row("6", "Saturn", "gas", 9.54, 9.449),Row("7", "Uranus", "ice", 19.22, 4.007),Row("8", "Neptune", "ice", 30.06, 3.883),], planetSchema)

planetsDF.write.format("cosmos.oltp").options(**cosmosConfig).mode("append").save()

# COMMAND ----------

display(planetsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://gsethistorageaccount.blob.core.windows.net/images/Mars_015524_pipp_lapl5_ap17-wavelet.jpg'>
# MAGIC <img src='https://gsethistorageaccount.blob.core.windows.net/images/Neptune_014046_pipp_lapl5_ap5-wavelet.jpg'>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the records we just wrote to CosmosDB

# COMMAND ----------

# Connect to CosmosDB and read the container directly

newPlanetsDF = spark.read.format("cosmos.oltp")\
  .options(**cosmosConfig)\
  .option("spark.cosmos.read.inferSchema.enabled", "true")\
  .load()

newPlanetsDF.selectExpr("*").show()

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://gsethistorageaccount.blob.core.windows.net/images/Neptune-zoom.JPG'>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write next record

# COMMAND ----------

dwarfDF = spark.createDataFrame([Row("9", "Pluto", "dwarf", 39.48, 0.37)], planetSchema)
dwarfDF.write.format("cosmos.oltp").options(**cosmosConfig).mode("append").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the table again

# COMMAND ----------

# Reuse the previous query and CosmosDB connection settings

newPlanetsDF.selectExpr("*").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Custom Query
# MAGIC By default, the Cosmos connector will use predicate push down which is the recommended approach and will take advantage of Spark's query plan optimisation engine. For predicates that can't be pushed down (some aggregates like count, sum etc) then you can send a query directly. Modify the readConfig with the customQuery attribute.

# COMMAND ----------

# SECRETS - Demo use only - these should be stored in AKV or a Secret Scope
readConfig = {
 "spark.cosmos.accountEndpoint" : "https://gsethi-cosmosdb.documents.azure.com:443/",
 "spark.cosmos.accountKey" : "7AgSLseb011NTcHhYYVwSHyq8nSUa9NWsoYigF37w43pEePiosUsUvaNB4gPhrieBlkgSjCo2cIyuxMdwUtScw==",
 "spark.cosmos.database" : "gsethicosmosdb",
 "spark.cosmos.container" : "planets",
 "spark.cosmos.read.customQuery" : "SELECT count(c.name) AS qty, c.type FROM c GROUP BY c.type"
}



# COMMAND ----------

customPlanetsDF = spark.read.format("cosmos.oltp")\
  .options(**readConfig)\
  .option("spark.cosmos.read.inferSchema.enabled", "true")\
  .load()

customPlanetsDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete the container

# COMMAND ----------

spark.sql("drop table cosmosCatalog.{}.{}".format(cosmosDatabase, cosmosContainer))
