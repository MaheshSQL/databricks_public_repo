# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading and Writing to Synapse
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Describe the connection architecture of Synapse and Spark
# MAGIC * Configure a connection between Databricks and Synapse
# MAGIC * Read data from Synapse
# MAGIC * Write data to Synapse
# MAGIC 
# MAGIC ### Azure Synapse
# MAGIC - leverages massively parallel processing (MPP) to quickly run complex queries across petabytes of data
# MAGIC - PolyBase T-SQL queries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Synapse Connector
# MAGIC - uses Azure Blob Storage as intermediary
# MAGIC - uses PolyBase in Synapse
# MAGIC - enables MPP reads and writes to Synapse from Azure Databricks
# MAGIC 
# MAGIC Note: The Synapse connector is more suited to ETL than to interactive queries. For interactive and ad-hoc queries, data should be extracted into a Databricks Delta table.
# MAGIC 
# MAGIC ```
# MAGIC                            ┌─────────┐
# MAGIC       ┌───────────────────>│ STORAGE │<──────────────────┐
# MAGIC       │ Storage acc key /  │ ACCOUNT │ Storage acc key / │
# MAGIC       │ Managed Service ID └─────────┘ OAuth 2.0         │
# MAGIC       │                         │                        │
# MAGIC       │                         │ Storage acc key /      │
# MAGIC       │                         │ OAuth 2.0              │
# MAGIC       v                         v                 ┌──────v────┐
# MAGIC ┌──────────┐              ┌──────────┐            │┌──────────┴┐
# MAGIC │ Synapse  │              │  Spark   │            ││ Spark     │
# MAGIC │ Analytics│<────────────>│  Driver  │<───────────>| Executors │
# MAGIC └──────────┘  JDBC with   └──────────┘ Configured  └───────────┘
# MAGIC               username &               in Spark
# MAGIC               password
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## SQL DW Connection
# MAGIC 
# MAGIC Three connections are made to exchange queries and data between Databricks and Synapse
# MAGIC 1. **Spark driver to Synapse**
# MAGIC    - the Spark driver connects to Synapse via JDBC using a username and password
# MAGIC 2. **Spark driver and executors to Azure Blob Storage**
# MAGIC    - the Azure Blob Storage container acts as an intermediary to store bulk data when reading from or writing to Synapse
# MAGIC    - Spark connects to the Blob Storage container using the Azure Blob Storage connector bundled in Databricks Runtime
# MAGIC    - the URI scheme for specifying this connection must be wasbs
# MAGIC    - the credential used for setting up this connection must be a storage account access key
# MAGIC    - the account access key is set in the session configuration associated with the notebook that runs the command
# MAGIC    - this configuration does not affect other notebooks attached to the same cluster. `spark` is the SparkSession object provided in the notebook
# MAGIC 3. **Synapse to Azure Blob Storage**
# MAGIC    - Synapse also connects to the Blob Storage container during loading and unloading of temporary data
# MAGIC    - set `forwardSparkAzureStorageCredentials` to true
# MAGIC    - the forwarded storage access key is represented by a temporary database scoped credential in the Synapse instance
# MAGIC    - Synapse connector creates a database scoped credential before asking Synapse to load or unload data
# MAGIC    - then it deletes the database scoped credential once the loading or unloading operation is done.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Configuration
# MAGIC 
# MAGIC ### Create Azure Blob Storage
# MAGIC Follow these steps to [create an Azure Storage Account](https://docs.microsoft.com/en-us/azure/storage/common/storage-quickstart-create-account?tabs=azure-portal#regenerate-storage-access-keys) and Container. The Synapse connector will use a [Shared Key](https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key) for authorization. Be sure to make note of the **Storage Account Name**, the **Container Name**, and the **Access Key** while working through these steps:
# MAGIC 
# MAGIC 1. Access the Azure Portal
# MAGIC 2. Create a New Resource
# MAGIC 3. Create a Storage account
# MAGIC 4. Make sure to specify the correct *Resource Group* and *Region*. Use any unique string as the  for the **Storage Account Name**
# MAGIC 5. Access Blobs
# MAGIC 6. Create a New Container using any unique string for the **Container Name**
# MAGIC 7. Retrieve the primary **Access Key** for the new Storage Account

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC In the cell below, enter the **Storage Account Name**, the **Container Name**, and the **Access Key**.

# COMMAND ----------

storageAccount = dbutils.secrets.get(scope="demo", key="storageaccount")
containerName = "polybase"
accessKey = dbutils.secrets.get(scope="demo", key="storagekey")

spark.conf.set(f"fs.azure.account.key.{storageAccount}.blob.core.windows.net", accessKey)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Configuration
# MAGIC 
# MAGIC ### Create an Azure Synapse Instance
# MAGIC Follow these steps to [create an Azure Storage Account](https://docs.microsoft.com/en-us/azure/storage/common/storage-quickstart-create-account?tabs=azure-portal#regenerate-storage-access-keys) and Container. The Synapse connector will use a [Shared Key](https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key) for authorization. Be sure to make note of the **Storage Account Name**, the **Container Name**, and the **Access Key** while working through these steps:
# MAGIC 
# MAGIC 1. Access the Azure Portal
# MAGIC 2. Create a New Resource
# MAGIC 3. Create a Synapse instance using these attributes:
# MAGIC    - Use any string for the **Database Name**
# MAGIC    - Select "Sample" as the Source
# MAGIC    - Select an existing or create a new SQL Server
# MAGIC 5. Access the new Synapse instance
# MAGIC 6. Select Query Editor (preview) and enter the proper credentials
# MAGIC 7. Run these two queries:
# MAGIC    - Create a Master Key in the SQL DW. This facilitates the SQL DW connection
# MAGIC 
# MAGIC      `CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'CORRECT-horse-battery-staple';`
# MAGIC 
# MAGIC    - Use a CTAS to create a staging table for the Customer Table. This query will create an empty table with the same schema as the Customer Table.
# MAGIC 
# MAGIC      ```
# MAGIC      CREATE TABLE dbo.DimCustomerStaging
# MAGIC      WITH
# MAGIC      ( DISTRIBUTION = ROUND_ROBIN, CLUSTERED COLUMNSTORE INDEX )
# MAGIC      AS
# MAGIC      SELECT  *
# MAGIC      FROM dbo.DimCustomer
# MAGIC      WHERE 1 = 2
# MAGIC      ;
# MAGIC      ```
# MAGIC 7. Access Connection Strings.
# MAGIC 8. Select JDBC and copy the **JDBC URI**.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC In the cell below, we'll access the JDBC URI stored securely in Key Vault.
# MAGIC Note that the table we will be using has already been defined.

# COMMAND ----------

tableName = "dbo.DimCustomer"
jdbcURI = dbutils.secrets.get(scope="demo", key="jdbc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read from the Customer Table
# MAGIC 
# MAGIC Next, use the Synapse Connector to read data from the Customer Table.
# MAGIC 
# MAGIC Use the read to define a tempory table that can be queried.
# MAGIC 
# MAGIC Note:
# MAGIC 
# MAGIC - the connector uses a caching directory on the Azure Blob Container.
# MAGIC - `forwardSparkAzureStorageCredentials` is set to `true` so that the Synapse instance can access the blob for its MPP read via Polybase

# COMMAND ----------

cacheDir = f"wasbs://{containerName}@{storageAccount}.blob.core.windows.net/cacheDir"

customerDF = (spark.read
  .format("com.databricks.spark.sqldw")
  .option("url", jdbcURI)
  .option("tempDir", cacheDir)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", tableName)
  .load())

customerDF.createOrReplaceTempView("customer_data")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Use SQL queries to count the number of rows in the Customer table and to display table metadata.

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from customer_data

# COMMAND ----------

# MAGIC %sql
# MAGIC describe customer_data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Note that `CustomerKey` and `CustomerAlternateKey` use a very similar naming convention.

# COMMAND ----------

# MAGIC %sql
# MAGIC select CustomerKey, CustomerAlternateKey from customer_data limit 10;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC In a situation in which we may be merging many new customers into this table, we can imagine that we may have issues with uniqueness with regard to the `CustomerKey`. Let us redefine `CustomerAlternateKey` for stronger uniqueness using a [UUID](https://en.wikipedia.org/wiki/Universally_unique_identifier).
# MAGIC 
# MAGIC To do this we will define a UDF and use it to transform the `CustomerAlternateKey` column. Once this is done, we will write the updated Customer Table to a Staging table.
# MAGIC 
# MAGIC **Note:** It is a best practice to update the Synapse instance via a staging table.

# COMMAND ----------

import uuid

from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

uuidUdf = udf(lambda : str(uuid.uuid4()), StringType())
customerUpdatedDF = customerDF.withColumn("CustomerAlternateKey", uuidUdf())
display(customerUpdatedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Use the Polybase Connector to Write to the Staging Table

# COMMAND ----------

(customerUpdatedDF.write
  .format("com.databricks.spark.sqldw")
  .mode("overwrite")
  .option("url", jdbcURI)
  .option("forward_spark_azure_storage_credentials", "true")
  .option("dbtable", tableName + "Staging")
  .option("tempdir", cacheDir)
  .save())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read and Display Changes from Staging Table

# COMMAND ----------

customerTempDF = (spark.read
  .format("com.databricks.spark.sqldw")
  .option("url", jdbcURI)
  .option("tempDir", cacheDir)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", tableName + "Staging")
  .load())

customerTempDF.createOrReplaceTempView("customer_temp_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC select CustomerKey, CustomerAlternateKey from customer_temp_data limit 10;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>