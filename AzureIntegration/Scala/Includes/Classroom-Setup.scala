// Databricks notebook source
// MAGIC 
// MAGIC %run "./Course-Name"

// COMMAND ----------

// MAGIC %run "./Dataset-Mounts"

// COMMAND ----------

// MAGIC %run "./Create-User-DB"

// COMMAND ----------

// // Utility method to count & print the number of records in each partition.
// def printRecordsPerPartition(df:org.apache.spark.sql.Dataset[Row]):Unit = {
//   import org.apache.spark.sql.functions._
//   println("Per-Partition Counts:")
//   val results = df.rdd                                   // Convert to an RDD
//     .mapPartitions(it => Array(it.size).iterator, true)  // For each partition, count
//     .collect()                                           // Return the counts to the driver

//   results.foreach(x => println("* " + x))
// }

// COMMAND ----------

// MAGIC %python
// MAGIC displayHTML("All done!")