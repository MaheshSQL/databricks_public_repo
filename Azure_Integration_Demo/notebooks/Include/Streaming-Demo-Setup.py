# Databricks notebook source
# MAGIC %run ./Classroom-Setup

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC dbutils.fs.rm(userhome + "/streaming-demo", true)
# MAGIC 
# MAGIC spark.read
# MAGIC   .json("/mnt/training/definitive-guide/data/activity-data-with-geo.json/")
# MAGIC   .toJSON
# MAGIC   .withColumnRenamed("value", "body")
# MAGIC   .write
# MAGIC   .mode("overwrite")
# MAGIC   .format("delta")
# MAGIC   .save(userhome + "/streaming-demo")
# MAGIC 
# MAGIC val activityStreamDF = (spark.readStream
# MAGIC   .format("delta")
# MAGIC   .option("maxFilesPerTrigger", 1)
# MAGIC   .load(userhome + "/streaming-demo")
# MAGIC )
