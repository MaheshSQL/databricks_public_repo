# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Build And Manage Your Data Lake With Delta Lake: Demo
# MAGIC 
# MAGIC ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Big data challenge #1: Data lakes can be messy, siloed, and slow
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/bi-and-ml-on-all-data.png" alt='Make all your data ready for BI and ML' width=1000/>
# MAGIC 
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/dl1.png" width=800/>
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/dl2.png" width=800/>
# MAGIC 
# MAGIC 
# MAGIC #### Reliability Issues with Data Lakes
# MAGIC 
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/projects_failing_reasons.png?raw=true" width=1000/>
# MAGIC 
# MAGIC 
# MAGIC - When a job fails, incomplete work is not rolled back, as it would be in a relational database.  Data may be left in an inconsistent state.  This issue is extremely difficult to deal with in production.
# MAGIC 
# MAGIC - Data lakes typically cannot enforce schema.  This is often touted as a "feature" called "schema-on-read," because it allows flexibility at data ingest time.  However, when downstream jobs fail trying to read corrupt data, we have a very difficult recovery problem.  It is often difficult just to find the source application that caused the problem... which makes fixing the problem even harder!
# MAGIC 
# MAGIC - Relational databases allow multiple concurrent users, and ensure that each user gets a consistent view of data.  Half-completed transactions never show up in the result sets of other concurrent users.  This is not true in a typical data lake.  Therefore, it is almost impossible to have a concurrent mix of read jobs and write jobs.  This becomes an even bigger problem with streaming data, because streams typically don't pause to let other jobs run!
# MAGIC 
# MAGIC #### Performance Issues with Data Lakes
# MAGIC 
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/projects_failing_reasons_1.png?raw=true" width=1000/>
# MAGIC 
# MAGIC - We have already noted that data lakes cannot provide a consistent view of data to concurrent users.  This is a reliability problem, but it is also a __*performance*__ problem because if we must run jobs one at a time, our production time window becomes extremely limited.
# MAGIC 
# MAGIC - Most data lake engineers have come face-to-face with the "small-file problem."  Data is typically ingested into a data lake in batches.  Each batch typically becomes a separate physical file in a directory that defines a table in the lake.  Over time, the number of physical files can grow to be very large.  When this happens, performance suffers because opening and closing these files is a time-consuming operation.  
# MAGIC 
# MAGIC - Experienced relational database architects may be surprised to learn that Big Data usually cannot be indexed in the same way as relational databases.  The indexes become too large to be manageable and performant.  Instead, we "partition" data by putting it into sub-directories.  Each partition can represent a column (or a composite set of columns) in the table.  This lets us avoid scanning the entire data set... *if* our queries are based on the partition column.  However, in the real world, analysts are running a wide range of queries which may or may not be based on the partition column.  In these scenarios, there is no benefit to partitioning.  In addition, partitioning breaks down if we choose a partition column with extremely high cardinality.
# MAGIC 
# MAGIC - Data lakes typically live in cloud storage (e.g., S3 on AWS, ADLS on Azure), and these storage devices are quite slow compared to SSD disk drives.  Most data lakes have no capability to cache data on faster devices, and this fact has a major impact on performance.
# MAGIC 
# MAGIC __*Delta Lake was built to solve these reliability and performance problems.*__  First, let's consider how Delta Lake addresses *reliability* issues...
# MAGIC 
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/delta_reliability.png?raw=true" width=1000/>
# MAGIC 
# MAGIC Note the Key Features in the graphic above.  We'll be diving into all of these capabilities as we go through the Workshop:
# MAGIC 
# MAGIC - __ACID Transactions:__ Delta Lake ACID compliance ensures that half-completed transactions are never persisted in the Lake, and concurrent users never see other users' in-flight transactions.
# MAGIC 
# MAGIC - __Mutations:__ Experienced relational database architects may be surprised to learn that most data lakes do not support updates and deletes.  These lakes concern themselves only with data ingest, which makes error correction and backfill very difficult.  In contrast, Delta Lake provides full support for Inserts, Updates, and Deletes.
# MAGIC 
# MAGIC - __Schema Enforcement:__ Delta Lake provides full support for schema enforcement at write time, greatly increasing data reliability.
# MAGIC 
# MAGIC - __Unified Batch and Streaming:__ Streaming data is becoming an essential capability for all enterprises.  We'll see how Delta Lake supports both batch and streaming modes, and in fact blurs the line between them, enabling architects to design systems that use both batch and streaming capabilities simultaneously.
# MAGIC 
# MAGIC - __Time Travel:__ unlike most data lakes, Delta Lake enables queries of data *as it existed* at a specific point in time.  This has important ramifications for reliability, error recovery, and synchronization with other systems, as we shall see later in this Workshop.
# MAGIC 
# MAGIC We have seen how Delta Lake enhances reliability.  Next, let's see how Delta Lake optimizes __*performance*__...
# MAGIC 
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/delta_performance.png?raw=true" width=1000/>
# MAGIC 
# MAGIC Again, we'll be diving into all these capabilities throughout the Workshop.  We'll be concentrating especially on features that are only available in Databricks' distribution of Delta Lake...
# MAGIC 
# MAGIC - __Compaction:__ Delta Lake provides sophisticated capabilities to solve the "small-file problem" by compacting small files into larger units.
# MAGIC 
# MAGIC - __Caching:__ Delta Lake transparently caches data on the SSD drives of worker nodes in a Spark cluster, greatly improving performance.
# MAGIC 
# MAGIC - __Data Skipping:__ this Delta Lake feature goes far beyond the limits of mere partitioning.
# MAGIC 
# MAGIC - __Z-Ordering:__ this is a brilliant alternative to traditional indexing, and further enhances Delta Lake performance.
# MAGIC 
# MAGIC Now that we have introduced the value proposition of Delta Lake, let's get a deeper understanding of the overall "Data Lake" concept.
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/dl3.png" width=800/>
# MAGIC 
# MAGIC 
# MAGIC ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Delta Architecture
# MAGIC 
# MAGIC Delta Lake is an open-source storage layer that brings ACID transactions to Apache Spark™ and big data workloads.
# MAGIC 
# MAGIC <img src="https://delta.io/wp-content/uploads/2019/04/Delta-Lake-marketecture-0423c.png" width=1012/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Demo: Building a reliable data pipeline with Delta Lake
# MAGIC 
# MAGIC For this demo, we will use a public data set of loans from Lending Club. It includes all funded loans from 2012 to 2017. Each loan includes demographic information provided by the applicant, as well as the current loan status (Current, Late, Fully Paid, etc.) and latest payment information. For a full view of the data please view the data dictionary available [here](https://resources.lendingclub.com/LCDataDictionary.xlsx).
# MAGIC 
# MAGIC 
# MAGIC ![Loan_Data](https://preview.ibb.co/d3tQ4R/Screen_Shot_2018_02_02_at_11_21_51_PM.png)
# MAGIC 
# MAGIC https://www.kaggle.com/wendykan/lending-club-loan-data

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Overview
# MAGIC <sp>
# MAGIC 1. **BRONZE** - Read raw data from Parquet files using Spark, save data in Delta Lake Bronze table
# MAGIC 3. **SILVER** - Perform ETL to clean and conform our data, saving the result as a Silver table
# MAGIC 4. **GOLD** - Load the Silver table, then narrow it down to fit our specific use case, saving the result as a Gold table
# MAGIC 5. Use the Gold table to demonstrate the features of Delta Lake.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Batch Processing

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Import raw data and save it into a Delta Lake table (Bronze)
# MAGIC * This will create a lot of small Parquet files emulating the typical small file problem that occurs with streaming or highly transactional data

# COMMAND ----------

# Read Parquet files with Spark
df = spark.read.parquet("/databricks-datasets/samples/lending_club/parquet/")

# COMMAND ----------

# Reduce the amount of data (to run on DBCE)
#(df, _) = df.randomSplit([0.025, 0.975], seed=123)

# Set up managed database to use
spark.sql("CREATE DATABASE IF NOT EXISTS gsethi_demo")
spark.sql('USE gsethi_demo')

# Tidy up paths and tables in case notebook has been run before and they already exist
dbutils.fs.rm("/Users/gurpreet.sethi@databricks.com/DeltaDemo/bronze_loan_stats", recurse=True)
dbutils.fs.rm("/Users/gurpreet.sethi@databricks.com/DeltaDemo/silver_loan_stats", recurse=True)
dbutils.fs.rm("/Users/gurpreet.sethi@databricks.com/DeltaDemo/gold_loan_by_state", recurse=True)
dbutils.fs.rm("/Users/gurpreet.sethi@databricks.com/DeltaDemo/loan_by_state.parquet", recurse=True)
spark.sql('DROP TABLE IF EXISTS bronze_loan_stats')
spark.sql("DROP TABLE IF EXISTS silver_loan_stats")
spark.sql('DROP TABLE IF EXISTS gold_loan_stats')

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://databricks.com/wp-content/uploads/2020/02/simplysaydelta.png" width=800/>

# COMMAND ----------

# MAGIC %fs mounts

# COMMAND ----------

# Configure destination path
DELTALAKE_BRONZE_PATH = "/Users/gurpreet.sethi@databricks.com/DeltaDemo/bronze_loan_stats"

# Write out the table
df.write.format('delta').mode('overwrite').save(DELTALAKE_BRONZE_PATH)

# Register the SQL table in the database
spark.sql(f"CREATE TABLE bronze_loan_stats USING delta LOCATION '{DELTALAKE_BRONZE_PATH}'") 

# Read the table
loan_stats = spark.read.format("delta").load(DELTALAKE_BRONZE_PATH)

display(loan_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ### What does Delta Log look like?

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY gsethi_demo.bronze_loan_stats

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED gsethi_demo.bronze_loan_stats

# COMMAND ----------

# MAGIC %fs ls /Users/gurpreet.sethi@databricks.com/DeltaDemo/bronze_loan_stats/

# COMMAND ----------

# DBTITLE 1,ETL - Filter Data and Fix Schema
from pyspark.sql.functions import *

# Selecting only the columns we are interested in
loan_stats = loan_stats.select("loan_status", "int_rate", "revol_util", "issue_d", "earliest_cr_line", "emp_length", "verification_status", \
                               "total_pymnt", "loan_amnt", "grade", "annual_inc", "dti", "addr_state", "term", "home_ownership", "purpose", \
                               "application_type", "delinq_2yrs", "total_acc")

# Creating 'bad_loan' label, which includes charged off, defaulted, and late repayments on loans
loan_stats = loan_stats.filter(loan_stats.loan_status.isin(["Default", "Charged Off", "Fully Paid"])) \
                       .withColumn("bad_loan", (~(loan_stats.loan_status == "Fully Paid")).cast("string"))

# Transforming string columns into numeric columns
loan_stats = loan_stats.withColumn('int_rate', regexp_replace('int_rate', '%', '').cast('float')) \
                       .withColumn('revol_util', regexp_replace('revol_util', '%', '').cast('float')) \
                       .withColumn('issue_year',  substring(loan_stats.issue_d, 5, 4).cast('double') ) \
                       .withColumn('earliest_year', substring(loan_stats.earliest_cr_line, 5, 4).cast('double'))

# Converting emp_length into numeric column
loan_stats = loan_stats.withColumn('emp_length', trim(regexp_replace(loan_stats.emp_length, "([ ]*+[a-zA-Z].*)|(n/a)", "") ))
loan_stats = loan_stats.withColumn('emp_length', trim(regexp_replace(loan_stats.emp_length, "< 1", "0") ))
loan_stats = loan_stats.withColumn('emp_length', trim(regexp_replace(loan_stats.emp_length, "10\\+", "10") ).cast('float'))

# Bucketing verification_status values together
loan_stats = loan_stats.withColumn('verification_status', trim(regexp_replace(loan_stats.verification_status, 'Source Verified', 'Verified')))

# Calculating the 'credit_length_in_years' column
loan_stats = loan_stats.withColumn('credit_length_in_years', (loan_stats.issue_year - loan_stats.earliest_year))

# Calculating the 'net' column, the total amount of money earned or lost per loan
loan_stats = loan_stats.withColumn('net', round(loan_stats.total_pymnt - loan_stats.loan_amnt, 2))

print('ETL code completed!')

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Save our cleaned and conformed table as a Silver table in Delta Lake

# COMMAND ----------

# Configure destination path
DELTALAKE_SILVER_PATH = "/Users/gurpreet.sethi@databricks.com/DeltaDemo/silver_loan_stats"

# Write out the table
loan_stats.write.format('delta').mode('overwrite').save(DELTALAKE_SILVER_PATH)

# Register the SQL table in the database
spark.sql("CREATE TABLE if not exists silver_loan_stats USING DELTA LOCATION '" + DELTALAKE_SILVER_PATH + "'")

# Read the table
loan_stats = spark.read.format("delta").load(DELTALAKE_SILVER_PATH)

display(loan_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Create Gold table
# MAGIC Now that our Silver table has been cleaned and conformed, and we've evolved the schema, the next step is to create a Gold table. Gold tables are often created to provide clean, reliable data for a specific business unit or use case.
# MAGIC 
# MAGIC In our case, we'll create a Gold table that includes only 2 columns - `addr_state` and `count` - to provide an aggregated view of our data. For our purposes, this table will allow us to show what Delta Lake can do, but in practice a table like this could be used to feed a downstream reporting or BI tool that needs data formatted in a very specific way. Silver tables often feed multiple downstream Gold tables.

# COMMAND ----------

# Aggregate the data
loan_by_state = loan_stats.groupBy("addr_state").count()

# Configure destination path
DELTALAKE_GOLD_PATH = "/Users/gurpreet.sethi@databricks.com/DeltaDemo/gold_loan_by_state"

# Write out the table
loan_by_state.write.format('delta').save(DELTALAKE_GOLD_PATH)

# Register the SQL table in our database
spark.sql(f"CREATE TABLE gold_loan_stats USING delta LOCATION '{DELTALAKE_GOLD_PATH}'")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT addr_state, sum(`count`) AS loans
# MAGIC FROM gold_loan_stats
# MAGIC GROUP BY addr_state

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stop the notebook before the streaming cell, in case of a "run all" 

# COMMAND ----------

dbutils.notebook.exit("stop") 

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Unified Batch and Streaming Source and Sink
# MAGIC 
# MAGIC These cells showcase streaming and batch concurrent queries (inserts and reads)
# MAGIC * This notebook will run an `INSERT` every 10s against our `loan_stats_delta` table
# MAGIC * We will run two streaming queries concurrently against this data
# MAGIC * Note, you can also use `writeStream` but this version is easier to run in DBCE

# COMMAND ----------

# Read the insertion of data
loan_by_state_readStream = spark.readStream.format("delta").load(DELTALAKE_GOLD_PATH)
loan_by_state_readStream.createOrReplaceTempView("loan_by_state_readStream")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Aggregation Query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT addr_state, sum(`count`) AS loans
# MAGIC FROM loan_by_state_readStream
# MAGIC GROUP BY addr_state

# COMMAND ----------

# MAGIC %md **Wait** until the stream is up and running before executing the code below

# COMMAND ----------

# MAGIC %md
# MAGIC #### Simulating a Streaming scenario - This could be a Kafka/ Kinesis/ Event Hub based input as well

# COMMAND ----------

import time
i = 1
while i <= 6:
  # Execute Insert statement
  insert_sql = "INSERT INTO gold_loan_stats VALUES ('IA', 45000)"
  spark.sql(insert_sql)
  print('gold_loan_stats: inserted new row of data, loop: [%s]' % i)
    
  # Loop through
  i = i + 1
  time.sleep(2)

# COMMAND ----------

# MAGIC %md 
# MAGIC **Note**: Once the previous cell is finished and the state of Iowa is fully populated in the map (in cell 26), click *Cancel* in Cell 26 to stop the `readStream`.

# COMMAND ----------

# MAGIC %md
# MAGIC Let's review our current set of loans using our map visualization.

# COMMAND ----------

# MAGIC %md Observe that the Iowa (middle state) has the largest number of loans due to the recent stream of data.  Note that the original `gold_loan_stats` table is updated as we're reading `loan_by_state_readStream`.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Full DML Support
# MAGIC 
# MAGIC **Note**: Full DML Support is a feature also in Delta Lake
# MAGIC 
# MAGIC Delta Lake supports standard DML including UPDATE, DELETE and MERGE INTO providing developers more controls to manage their big datasets.

# COMMAND ----------

# MAGIC %md Let's start by creating a traditional Parquet table

# COMMAND ----------

# MAGIC %fs ls /Users/gurpreet.sethi@databricks.com/DeltaDemo/loan_by_state.parquet

# COMMAND ----------

# Load new DataFrame based on current Delta table

lbs_df = sql("SELECT * FROM gold_loan_stats")

PARQUET_TABLE_PATH = "/Users/gurpreet.sethi@databricks.com/DeltaDemo/loan_by_state.parquet"

dbutils.fs.rm("/Users/gurpreet.sethi@databricks.com/DeltaDemo/loan_by_state.parquet/", recurse=True)

# Save DataFrame to Parquet
lbs_df.write.mode("overwrite").parquet(PARQUET_TABLE_PATH)

# Reload Parquet Data
#lbs_pq = spark.read.parquet("/Users/gurpreet.sethi@databricks.com/DeltaDemo/loan_by_state.parquet")

# Create new table on this parquet data
#lbs_pq.createOrReplaceTempView("loan_by_state_parquet")


# COMMAND ----------

# Review data
sqlCreate = f"DROP TABLE IF EXISTS loan_parquet"
spark.sql(sqlCreate)

sqlCreate = f"create table loan_parquet using PARQUET LOCATION '{PARQUET_TABLE_PATH}' " 
spark.sql(sqlCreate)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED loan_parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC select addr_state, count(*) from loan_parquet WHERE addr_state = 'IA' group by addr_state

# COMMAND ----------

# MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) DELETE Support
# MAGIC 
# MAGIC The data was originally supposed to be assigned to `WA` state, so let's `DELETE` those values assigned to `IA`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Attempting to run `DELETE` on the Parquet table
# MAGIC DELETE FROM loan_parquet WHERE addr_state = 'IA'

# COMMAND ----------

# MAGIC %md **Note**: This command fails because the `DELETE` statements are not supported in Parquet, but are supported in Delta Lake.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Running `DELETE` on the Delta Lake table to remove records from Iowa
# MAGIC DELETE FROM gold_loan_stats
# MAGIC WHERE addr_state = 'IA'

# COMMAND ----------

# MAGIC %md
# MAGIC As you can see below, after running the `DELETE` command, records containing loans from Iowa have been successfully deleted.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Review current loans within the `gold_loan_stats` Delta Lake table
# MAGIC SELECT addr_state, SUM(`count`) AS loans
# MAGIC FROM gold_loan_stats
# MAGIC GROUP BY addr_state

# COMMAND ----------

# MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) UPDATE Support
# MAGIC The loan data that we assigned to `IA` was originally supposed to be assigned to `WA` state, so let's `UPDATE` those values.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Attempting to run `UPDATE` on the Parquet table
# MAGIC UPDATE loan_by_state_pq SET `count` = 2700 WHERE addr_state = 'WA'

# COMMAND ----------

# MAGIC %md **Note**: This command fails because the `UPDATE` statements are not supported in Parquet, but are supported in Delta Lake.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Running `UPDATE` on the Delta Lake table
# MAGIC UPDATE gold_loan_stats SET `count` = 2700 WHERE addr_state = 'WA'

# COMMAND ----------

# MAGIC %md As you can see, we successfully ran an `UPDATE` to move those loans from Iowa to Washington state.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Review current loans within the `gold_loan_stats` Delta Lake table
# MAGIC SELECT addr_state, SUM(`count`) AS loans
# MAGIC FROM gold_loan_stats
# MAGIC GROUP BY addr_state

# COMMAND ----------

# MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) MERGE INTO Support
# MAGIC 
# MAGIC #### INSERT or UPDATE with Parquet: 7-step process
# MAGIC 
# MAGIC With a legacy data pipeline, to insert or update a table, you must:
# MAGIC 1. Identify the new rows to be inserted
# MAGIC 2. Identify the rows that will be replaced (i.e. updated)
# MAGIC 3. Identify all of the rows that are not impacted by the insert or update
# MAGIC 4. Create a new temp based on all three insert statements
# MAGIC 5. Delete the original table (and all of those associated files)
# MAGIC 6. "Rename" the temp table back to the original table name
# MAGIC 7. Drop the temp table
# MAGIC 
# MAGIC ![](https://pages.databricks.com/rs/094-YMS-629/images/merge-into-legacy.gif)
# MAGIC 
# MAGIC #### INSERT or UPDATE with Delta Lake: 2-step process
# MAGIC 
# MAGIC With Delta Lake, inserting or updating a table is a simple 2-step process: 
# MAGIC 1. Identify rows to insert or update
# MAGIC 2. Use the `MERGE` command

# COMMAND ----------

# Let's create a simple table to merge
items = [('IA', 0), ('CA', 2500), ('OR', 0)]
cols = ['addr_state', 'count']
merge_table = spark.createDataFrame(items, cols)
merge_table.createOrReplaceTempView("merge_table")
display(merge_table)

# COMMAND ----------

# MAGIC %md Instead of writing separate `INSERT` and `UPDATE` statements, we can use a `MERGE` statement. 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO gold_loan_stats as d
# MAGIC USING merge_table as m
# MAGIC on d.addr_state = m.addr_state
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED 
# MAGIC   THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Review current loans within the `gold_loan_stats` Delta Lake table
# MAGIC SELECT addr_state, SUM(`count`) AS loans
# MAGIC FROM gold_loan_stats
# MAGIC GROUP BY addr_state

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Schema Evolution
# MAGIC With the `mergeSchema` option, you can evolve your Delta Lake table schema

# COMMAND ----------

# Generate new loans with dollar amounts 
loans = sql("SELECT addr_state, CAST(rand(10)*count AS bigint) AS count, CAST(rand(10) * 10000 * count AS double) AS amount FROM gold_loan_stats")
display(loans)

# COMMAND ----------

# Let's write this data out to our Delta table
loans.write.format("delta").mode("append").save(DELTALAKE_GOLD_PATH)

# COMMAND ----------

# MAGIC %md **Note**: The command above fails because the schema of our new data does not match the schema of our original data.
# MAGIC 
# MAGIC By adding the **mergeSchema** option, we can successfully migrate our schema, as shown below.

# COMMAND ----------

# Add the mergeSchema option
loans.write.option("mergeSchema","true").format("delta").mode("append").save(DELTALAKE_GOLD_PATH)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_loan_stats limit 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Review current loans within the `gold_loan_stats` Delta Lake table
# MAGIC SELECT addr_state, SUM(`amount`) 
# MAGIC FROM gold_loan_stats
# MAGIC GROUP BY addr_state
# MAGIC ORDER BY SUM(`amount`) DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Let's Travel back in Time!
# MAGIC Databricks Delta’s time travel capabilities simplify building data pipelines for the following use cases. 
# MAGIC 
# MAGIC * Audit Data Changes
# MAGIC * Reproduce experiments & reports
# MAGIC * Rollbacks
# MAGIC 
# MAGIC As you write into a Delta table or directory, every operation is automatically versioned.
# MAGIC 
# MAGIC You can query by:
# MAGIC 1. Using a timestamp
# MAGIC 1. Using a version number
# MAGIC 
# MAGIC using Python, Scala, and/or SQL syntax; for these examples we will use the SQL syntax.  
# MAGIC 
# MAGIC For more information, refer to [Introducing Delta Time Travel for Large Scale Data Lakes](https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html)

# COMMAND ----------

# MAGIC %md ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Review Delta Lake Table History
# MAGIC All the transactions for this table are stored within this table including the initial set of insertions, update, delete, merge, and inserts with schema modification

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY gold_loan_stats

# COMMAND ----------

# MAGIC %md ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Time Travel via Version Number
# MAGIC Below are SQL syntax examples of Delta Time Travel by using a Version Number

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_loan_stats VERSION AS OF 3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_loan_stats VERSION AS OF 0

# COMMAND ----------

# MAGIC %md
# MAGIC %md ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Build & Manage Your Data Lake With Delta Lake
# MAGIC 
# MAGIC To get started with Delta Lake, visit [delta.io](https://delta.io/).
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/dl4.png" width=800/>
