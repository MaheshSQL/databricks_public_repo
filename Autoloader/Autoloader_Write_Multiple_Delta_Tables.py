# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #                                                  Databricks Autoloader
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2020/02/autoloader.png" style="width: 350px; max-width: 80%; height: 100px" />

# COMMAND ----------

# DBTITLE 1,DELETE OLD SCHEMA, CHECKPOINT, BRONZE Location
dbutils.fs.rm(checkpoint_path,recurse=True)
dbutils.fs.rm(inbound_source_schema_path,recurse=True)
dbutils.fs.rm(bronze_mount_path,recurse=True)

# COMMAND ----------

# DBTITLE 1,Configure Path Variables For Incoming File Source,  Schema, Checkpoint and Bronze
from pyspark.sql.types import StructType,StructField, StringType
from pyspark.sql.functions import *

# Please change the below to a suitable location
inbound_source_path  = "dbfs:/FileStore/autoloader/landing/"
inbound_source_schema_path  = "dbfs:/FileStore/autoloader/schema"
bronze_mount_path   = "dbfs:/FileStore/autoloader/bronze/"
checkpoint_path = "dbfs:/FileStore/autoloader/checkpoint/"

# COMMAND ----------

# DBTITLE 1,Function To Process Incoming Micro-batches and Write To Multiple Delta Tables
def parse_membership(bronze_df: DataFrame,batchId: str):
  app_id = "Test_Autoloader"
  
  bronze_df = bronze_df.withColumn('Input_File_Name',input_file_name())
  bronze_df = bronze_df.select("Id","membershipId","serviceMDEData.*","Input_File_Name")
  
  member=bronze_df.selectExpr("id as member_pk","membershipId as membership_id","FName as First_Name","LName as Last_Name","Input_File_Name")
  member_address=bronze_df.selectExpr("id as member_pk","membershipId as membership_id","Address as Member_Address","Input_File_Name")
                     
  member.write.mode("append").format("delta").option("txnVersion", batchId).option("txnAppId", app_id).save(path=f"{bronze_mount_path}member")
  member_address.write.mode("append").format("delta").option("txnVersion", batchId).option("txnAppId", app_id).save(path=f"{bronze_mount_path}member_address")

  


# COMMAND ----------

# DBTITLE 1,Autoloader Code - To read input stream and use foreachbatch to refer function
df = spark.readStream.format('cloudFiles') \
  .option('cloudFiles.format', 'json') \
  .option('cloudFiles.schemaLocation',inbound_source_schema_path) \
  .option("cloudFiles.schemaEvolutionMode", "rescue") \
  .option("cloudFiles.inferColumnTypes", "true") \
  .option("cloudFiles.useIncrementalListing","true") \
  .option("maxFilesPerTrigger", 1) \
  .load(inbound_source_path)

df.writeStream \
     .format("delta") \
     .foreachBatch(parse_membership) \
     .option("checkpointLocation",checkpoint_path) \
     .trigger(once=True) \
    .start()

# COMMAND ----------

# DBTITLE 1,Display Results
display(spark.read.load(f"{bronze_mount_path}member"))
display(spark.read.load(f"{bronze_mount_path}member_address"))
