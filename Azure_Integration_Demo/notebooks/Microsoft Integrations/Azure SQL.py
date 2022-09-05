# Databricks notebook source
# MAGIC %md <a href='$../Azure Integrations Start Here'>Home</a>

# COMMAND ----------

# MAGIC %md
# MAGIC # Azure SQL Using Spark JDBC Connector

# COMMAND ----------

keyvault_secret_scope = "gsethi-kv-scope" # The Azure Key Vault Secret Scope
keyvault_sqldbatabase_username = "gsethi-sql-user" 
keyvault_sqldbatabase_password = "gsethi-sql-password" 

username = dbutils.secrets.get(scope = keyvault_secret_scope, key = keyvault_sqldbatabase_username) # The Synapse DW secret
password = dbutils.secrets.get(scope = keyvault_secret_scope, key = keyvault_sqldbatabase_password) # The Synapse DW secret

url = 'jdbc:sqlserver://gsethisqlserver.database.windows.net:1433;database=gsethi-sqldatabase;'
table_name = 'dbo.Capitals'


# COMMAND ----------

# Sample DataSet
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

data2 =  [("Canberra","Australia",2000000),
         ("London","United Kingdom",8900000),
         ("Washington","United States Of America",7500000),
         ("Tokyo","Japan",1300000)
         ]

schema = StructType([ \
    StructField("city",StringType(),True), \
    StructField("country",StringType(),True), \
    StructField("population", IntegerType(), True) \
  ])


df = spark.createDataFrame(data=data2,schema=schema)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

df.write \
    .format("com.microsoft.sqlserver.jdbc.spark") \
    .mode("overwrite") \
    .option("url", url) \
    .option("dbtable", table_name) \
    .option("user", username) \
    .option("password", password) \
    .save()



# COMMAND ----------

asqlDF = spark.read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("user", username) \
        .option("password", password).load()

# COMMAND ----------

display(asqlDF)

# COMMAND ----------

# MAGIC %md
# MAGIC **INSERT into Table**

# COMMAND ----------

# Sample DataSet
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

data2 =  [("Melbourne","Australia",5070000),
         ("sydney","Australia",5312000)]

schema = StructType([ \
    StructField("city",StringType(),True), \
    StructField("country",StringType(),True), \
    StructField("population", IntegerType(), True) \
  ])
 

# COMMAND ----------

df2 = spark.createDataFrame(data=data2,schema=schema)
df2.printSchema()
df2.show(truncate=False)


# COMMAND ----------

try:
  df2.write \
    .format("com.microsoft.sqlserver.jdbc.spark") \
    .mode("append") \
    .option("url", url) \
    .option("dbtable", table_name) \
    .option("user", username) \
    .option("password", password) \
    .save()
except ValueError as error :
    print("Connector write failed", error)

# COMMAND ----------

asqlDF = spark.read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("user", username) \
        .option("password", password).load()

# COMMAND ----------

display(asqlDF)
