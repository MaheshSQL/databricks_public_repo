# Databricks notebook source
# MAGIC %md <a href='$../Azure Integrations Start Here'>Home</a>

# COMMAND ----------

# MAGIC %md
# MAGIC # Dashboards

# COMMAND ----------

# This setup code is from the Delta Lake demo notebook (https://australiaeast.azuredatabricks.net/?o=7073202429053763#notebook/2185886547692372/command/3229056558800951)
lspq_path = "/databricks-datasets/samples/lending_club/parquet/"
data = spark.read.parquet(lspq_path)
(loan_stats, loan_stats_rest) = data.randomSplit([0.01, 0.99], seed=123)
loan_stats = loan_stats.select("addr_state", "loan_status")
loan_by_state = loan_stats.groupBy("addr_state").count()
loan_by_state.createOrReplaceTempView("loan_by_state")
DELTALAKE_SILVER_PATH = "/ml/loan_by_state_delta"
dbutils.fs.rm(DELTALAKE_SILVER_PATH, recurse=True)

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Current example is creating a new table instead of in-place import so will need to change this code
# MAGIC DROP TABLE IF EXISTS loan_by_state_delta;
# MAGIC 
# MAGIC CREATE TABLE loan_by_state_delta
# MAGIC USING delta
# MAGIC LOCATION '/ml/loan_by_state_delta'
# MAGIC AS SELECT * FROM loan_by_state;
# MAGIC 
# MAGIC -- View Delta Lake table
# MAGIC SELECT * FROM loan_by_state_delta

# COMMAND ----------

# Create a simple dataframe with a schema, and populate with data

from pyspark.sql.types import IntegerType, StringType, StructType, StructField

my_schema = StructType([
  StructField('Make', StringType(), True),
  StructField('Model', StringType(), True),
  StructField('Engines', IntegerType(), True)
])

my_data = [('Airbus', 'A300', 2),
           ('Airbus', 'A310', 2),
           ('Airbus', 'A320', 2),
           ('Airbus', 'A330', 2),
           ('Airbus', 'A340', 4),
           ('Airbus', 'A350', 2),
           ('Airbus', 'A380', 4),
           ('Boeing', 'B707', 4),
           ('Boeing', 'B717', 2),
           ('Boeing', 'B727', 3),
           ('Boeing', 'B737', 2),
           ('Boeing', 'B747', 4),
           ('Boeing', 'B757', 2),
           ('Boeing', 'B767', 2),
           ('Boeing', 'B777', 2),
           ('Boeing', 'B787', 2)]

airliners = spark.createDataFrame(my_data, schema = my_schema)
display(airliners)

# COMMAND ----------

airliners.createOrReplaceTempView('airliners')

# COMMAND ----------

# MAGIC %sql select make,engines, model from airliners
