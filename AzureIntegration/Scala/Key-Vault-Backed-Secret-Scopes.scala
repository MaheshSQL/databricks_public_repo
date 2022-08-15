// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC #![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Key Vault-Backed Secret Scopes
// MAGIC 
// MAGIC ## Learning Objectives
// MAGIC By the end of this lessons, you should be able to:
// MAGIC * Configure Databricks to access Key Vault secrets
// MAGIC * Read and write data directly from Blob Storage using secrets stored in Key Vault
// MAGIC * Set different levels of access permission using SAS at the Storage service level
// MAGIC * Mount Blob Storage into DBFS
// MAGIC * Describe how mounting impacts secure access to data
// MAGIC  
// MAGIC ### Online Resources
// MAGIC 
// MAGIC - [Azure Databricks Secrets](https://docs.azuredatabricks.net/user-guide/secrets/index.html)
// MAGIC - [Azure Key Vault](https://docs.microsoft.com/en-us/azure/key-vault/key-vault-whatis)
// MAGIC - [Azure Databricks DBFS](https://docs.azuredatabricks.net/user-guide/dbfs-databricks-file-system.html)
// MAGIC - [Introduction to Azure Blob storage](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction)
// MAGIC - [Databricks with Azure Blob Storage](https://docs.databricks.com/spark/latest/data-sources/azure/azure-storage.html)
// MAGIC - [Azure Data Lake Storage Gen1](https://docs.azuredatabricks.net/spark/latest/data-sources/azure/azure-datalake.html#mount-azure-data-lake)
// MAGIC - [Azure Data Lake Storage Gen2](https://docs.databricks.com/spark/latest/data-sources/azure/azure-datalake-gen2.html)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Classroom setup
// MAGIC 
// MAGIC A quick script to define a username variable in Python and Scala.

// COMMAND ----------

// MAGIC %run ./Includes/User-Name

// COMMAND ----------

// MAGIC %md
// MAGIC ### List Secret Scopes
// MAGIC 
// MAGIC To list the existing secret scopes the `dbutils.secrets` utility can be used.
// MAGIC 
// MAGIC You can list all scopes currently available in your workspace with:

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.secrets.listScopes()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### List Secrets within a specific scope
// MAGIC 
// MAGIC 
// MAGIC To list the secrets within a specific scope, you can supply that scope name.

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.secrets.list("demo")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Using your Secrets
// MAGIC 
// MAGIC To use your secrets, you supply the scope and key to the `get` method.
// MAGIC 
// MAGIC Run the following cell to retrieve and print a secret.

// COMMAND ----------

// MAGIC %python
// MAGIC print(dbutils.secrets.get(scope="demo", key="storageread"))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Secrets are not displayed in clear text
// MAGIC 
// MAGIC Notice that the value when printed out is `[REDACTED]`. This is to prevent your secrets from being exposed.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Mount Azure Blob Container - Read/List
// MAGIC 
// MAGIC In this section, we'll demonstrating using a `SASTOKEN` that only has list and read permissions managed at the Storage Account level.
// MAGIC 
// MAGIC **This means:**
// MAGIC - Any user within the workspace can view and read the files mounted using this key
// MAGIC - This key can be used to mount any container within the storage account with these privileges

// COMMAND ----------

// Unmount directory if previously mounted.
val MOUNTPOINT = "/mnt/commonfiles"

dbutils.fs.mounts.map(mnt => if (mnt.mountPoint == MOUNTPOINT) dbutils.fs.unmount(MOUNTPOINT) else None)

// Add the Storage Account, Container, and reference the secret to pass the SAS Token
val STORAGE_ACCOUNT = dbutils.secrets.get(scope="demo", key="storageaccount")
val CONTAINER = "commonfiles"
val SASTOKEN = dbutils.secrets.get(scope="demo", key="storageread")

//  Do not change these values
val SOURCE = s"wasbs://$CONTAINER@$STORAGE_ACCOUNT.blob.core.windows.net/"
val URI = s"fs.azure.sas.$CONTAINER.$STORAGE_ACCOUNT.blob.core.windows.net"

try
  {
    dbutils.fs.mount(
      source = SOURCE,
      mountPoint = MOUNTPOINT,
      extraConfigs = Map(URI -> SASTOKEN))
  } 
catch
  {
    case e: Throwable => println(e)
  }

display(dbutils.fs.ls(MOUNTPOINT))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Define and display a Dataframe that reads a file from the mounted directory

// COMMAND ----------

val salesDF = spark.read
  .option("header", true)
  .option("inferSchema", true)
  .csv(MOUNTPOINT + "/sales.csv")

display(salesDF)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Filter the Dataframe and display the results

// COMMAND ----------

import org.apache.spark.sql.functions.col

val sales2004DF = salesDF
  .filter((col("ShipDateKey") > 20031231) &&
          (col("ShipDateKey") <= 20041231))

display(sales2004DF)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Details....
// MAGIC 
// MAGIC 
// MAGIC While we can list and read files with this token, our job will abort when we try to write.

// COMMAND ----------

try
  {
    sales2004DF.write.mode("overwrite").parquet(MOUNTPOINT + "/sales2004")
  } 
catch
  {
  case e: Throwable => println(e)
  }

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Review
// MAGIC 
// MAGIC At this point you should see how to:
// MAGIC * Use Secrets to access blobstorage
// MAGIC * Mount the blobstore to dbfs (Data Bricks File System)
// MAGIC 
// MAGIC Mounting data to dbfs makes that content available to anyone in that workspace. 
// MAGIC 
// MAGIC If you want to access blob store directly without mounting the rest of the notebook demonstrate that process.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Writing Directly to Blob using SAS token
// MAGIC 
// MAGIC Note that when you mount a directory, by default, all users within the workspace will have the same privileges to interact with that directory. Here, we'll look at using a SAS token to directly write to a blob (without mounting). This ensures that only users with the workspace that have access to the associated key vault will be able to write.

// COMMAND ----------

spark.conf.set(URI, dbutils.secrets.get(scope="demo", key="storagewrite"))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Listing Directory Contents and writing using SAS token
// MAGIC 
// MAGIC Because the configured container SAS gives us full permissions, we can interact with the blob storage using our `dbutils.fs` methods.

// COMMAND ----------

display(dbutils.fs.ls(SOURCE))

// COMMAND ----------

// MAGIC %md
// MAGIC We can write to this blob directly, without exposing this mount to others in our workspace.

// COMMAND ----------

sales2004DF.write.mode("overwrite").parquet(SOURCE + "/sales2004")

// COMMAND ----------

display(dbutils.fs.ls(SOURCE))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Deleting using SAS token
// MAGIC 
// MAGIC This scope also has delete permissions.

// COMMAND ----------

dbutils.fs.rm(SOURCE + "/sales2004", true)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Cleaning up mounts
// MAGIC 
// MAGIC If you don't explicitly unmount, the read-only blob that you mounted at the beginning of this notebook will remain accessible in your workspace.

// COMMAND ----------

dbutils.fs.mounts.map(mnt => if (mnt.mountPoint == MOUNTPOINT) dbutils.fs.unmount(MOUNTPOINT) else None)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Congratulations!
// MAGIC 
// MAGIC You should now be able to use the following tools in your workspace:
// MAGIC 
// MAGIC * Databricks Secrets
// MAGIC * Azure Key Vault
// MAGIC * SAS token
// MAGIC * dbutils.mount

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>