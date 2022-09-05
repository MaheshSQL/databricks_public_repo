# Databricks notebook source
# MAGIC %md <a href='$../Azure Integrations Start Here'>Home</a>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://raw.githubusercontent.com/databricks/koalas/master/icons/koalas-logo.png' width=300>
# MAGIC 
# MAGIC pandas is a great tool to analyze small datasets on a single machine. When the need for bigger datasets arises, users often choose PySpark. However, the converting code from pandas to PySpark is not easy as PySpark APIs are considerably different from pandas APIs. Koalas makes the learning curve significantly easier by providing pandas-like APIs on the top of PySpark. With Koalas, users can take advantage of the benefits of PySpark with minimal efforts, and thus get to value much faster. With this package, you can:
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC <ul>
# MAGIC <li>Be immediately productive with Spark, with no learning curve, if you are already familiar with pandas.
# MAGIC <li>Have a single codebase that works both with pandas (tests, smaller datasets) and with Spark (distributed datasets).
# MAGIC   </ul>

# COMMAND ----------

# MAGIC %pip install koalas

# COMMAND ----------

# MAGIC %md
# MAGIC # 10 minutes to Koalas
# MAGIC 
# MAGIC This is a short introduction to Koalas, geared mainly for new users. This notebook shows you some key differences between pandas and Koalas. You can run this examples by yourself on a live notebook [here](https://mybinder.org/v2/gh/databricks/koalas/master?filepath=docs%2Fsource%2Fgetting_started%2F10min.ipynb). For Databricks Runtime, you can import and run [the current .ipynb file](https://raw.githubusercontent.com/databricks/koalas/master/docs/source/getting_started/10min.ipynb) out of the box.
# MAGIC 
# MAGIC Credit: https://koalas.readthedocs.io/en/latest/getting_started/10min.html
# MAGIC 
# MAGIC Customarily, we import Koalas as follows:

# COMMAND ----------

import pandas as pd
#import pyspark.pandas as pd
import numpy as np
import databricks.koalas as ks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Object Creation

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a Koalas Series by passing a list of values, letting Koalas create a default integer index:

# COMMAND ----------

s = ks.Series([1, 3, 5, np.nan, 6, 8])

# COMMAND ----------

s

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a Koalas DataFrame by passing a dict of objects that can be converted to series-like.

# COMMAND ----------

kdf = ks.DataFrame(
    {'a': [1, 2, 3, 4, 5, 6],
     'b': [100, 200, 300, 400, 500, 600],
     'c': ["one", "two", "three", "four", "five", "six"]},
    index=[10, 20, 30, 40, 50, 60])

# COMMAND ----------

kdf

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a pandas DataFrame by passing a numpy array, with a datetime index and labeled columns:

# COMMAND ----------

dates = pd.date_range('20130101', periods=6)

# COMMAND ----------

dates

# COMMAND ----------

pdf = pd.DataFrame(np.random.randn(6, 4), index=dates, columns=list('ABCD'))

# COMMAND ----------

pdf

# COMMAND ----------

# MAGIC %md
# MAGIC Now, this pandas DataFrame can be converted to a Koalas DataFrame

# COMMAND ----------

kdf = ks.from_pandas(pdf)

# COMMAND ----------

type(kdf)

# COMMAND ----------

# MAGIC %md
# MAGIC It looks and behaves the same as a pandas DataFrame though

# COMMAND ----------

kdf

# COMMAND ----------

# MAGIC %md
# MAGIC Also, it is possible to create a Koalas DataFrame from Spark DataFrame.  
# MAGIC 
# MAGIC Creating a Spark DataFrame from pandas DataFrame

# COMMAND ----------

sdf = spark.createDataFrame(pdf)

# COMMAND ----------

sdf.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Koalas DataFrame from Spark DataFrame.
# MAGIC `to_koalas()` is automatically attached to Spark DataFrame and available as an API when Koalas is imported.

# COMMAND ----------

kdf = sdf.to_koalas()

# COMMAND ----------

kdf

# COMMAND ----------

# MAGIC %md
# MAGIC Having specific [dtypes](http://pandas.pydata.org/pandas-docs/stable/basics.html#basics-dtypes) . Types that are common to both Spark and pandas are currently supported.

# COMMAND ----------

kdf.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Viewing Data
# MAGIC 
# MAGIC See the [API Reference](https://koalas.readthedocs.io/en/latest/reference/index.html).

# COMMAND ----------

# MAGIC %md
# MAGIC See the top rows of the frame. The results may not be the same as pandas though: unlike pandas, the data in a Spark dataframe is not _ordered_, it has no intrinsic notion of index. When asked for the head of a dataframe, Spark will just take the requested number of rows from a partition. Do not rely on it to return specific rows, use `.loc` or `iloc` instead.

# COMMAND ----------

kdf.head()

# COMMAND ----------

# MAGIC %md
# MAGIC Display the index, columns, and the underlying numpy data.
# MAGIC 
# MAGIC You can also retrieve the index; the index column can be ascribed to a DataFrame, see later

# COMMAND ----------

kdf.index

# COMMAND ----------

kdf.columns

# COMMAND ----------

kdf.to_numpy()

# COMMAND ----------

# MAGIC %md
# MAGIC Describe shows a quick statistic summary of your data

# COMMAND ----------

kdf.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC Transposing your data

# COMMAND ----------

kdf.T

# COMMAND ----------

# MAGIC %md
# MAGIC Sorting by its index

# COMMAND ----------

kdf.sort_index(ascending=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Sorting by value

# COMMAND ----------

kdf.sort_values(by='B')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Missing Data
# MAGIC Koalas primarily uses the value `np.nan` to represent missing data. It is by default not included in computations. 

# COMMAND ----------

pdf1 = pdf.reindex(index=dates[0:4], columns=list(pdf.columns) + ['E'])

# COMMAND ----------

pdf1.loc[dates[0]:dates[1], 'E'] = 1

# COMMAND ----------

kdf1 = ks.from_pandas(pdf1)

# COMMAND ----------

kdf1

# COMMAND ----------

# MAGIC %md
# MAGIC To drop any rows that have missing data.

# COMMAND ----------

kdf1.dropna(how='any')

# COMMAND ----------

# MAGIC %md
# MAGIC Filling missing data.

# COMMAND ----------

kdf1.fillna(value=5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Operations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stats
# MAGIC Operations in general exclude missing data.
# MAGIC 
# MAGIC Performing a descriptive statistic:

# COMMAND ----------

kdf.mean()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark Configurations
# MAGIC 
# MAGIC Various configurations in PySpark could be applied internally in Koalas.
# MAGIC For example, you can enable Arrow optimization to hugely speed up internal pandas conversion. See <a href="https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html">PySpark Usage Guide for Pandas with Apache Arrow</a>.

# COMMAND ----------

prev = spark.conf.get("spark.sql.execution.arrow.enabled")  # Keep its default value.
ks.set_option("compute.default_index_type", "distributed")  # Use default index prevent overhead.
import warnings
warnings.filterwarnings("ignore")  # Ignore warnings coming from Arrow optimizations.

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.enabled", True)
%timeit ks.range(300000).to_pandas()

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.enabled", False)
%timeit ks.range(300000).to_pandas()

# COMMAND ----------

ks.reset_option("compute.default_index_type")
spark.conf.set("spark.sql.execution.arrow.enabled", prev)  # Set its default value back.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grouping
# MAGIC By “group by” we are referring to a process involving one or more of the following steps:
# MAGIC 
# MAGIC - Splitting the data into groups based on some criteria
# MAGIC - Applying a function to each group independently
# MAGIC - Combining the results into a data structure

# COMMAND ----------

kdf = ks.DataFrame({'A': ['foo', 'bar', 'foo', 'bar',
                          'foo', 'bar', 'foo', 'foo'],
                    'B': ['one', 'one', 'two', 'three',
                          'two', 'two', 'one', 'three'],
                    'C': np.random.randn(8),
                    'D': np.random.randn(8)})

# COMMAND ----------

kdf

# COMMAND ----------

# MAGIC %md
# MAGIC Grouping and then applying the [sum()](https://koalas.readthedocs.io/en/latest/reference/api/databricks.koalas.groupby.GroupBy.sum.html#databricks.koalas.groupby.GroupBy.sum) function to the resulting groups.

# COMMAND ----------

kdf.groupby('A').sum()

# COMMAND ----------

# MAGIC %md
# MAGIC Grouping by multiple columns forms a hierarchical index, and again we can apply the sum function.

# COMMAND ----------

kdf.groupby(['A', 'B']).sum()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plotting
# MAGIC See the <a href="https://koalas.readthedocs.io/en/latest/reference/frame.html#plotting">Plotting</a> docs.

# COMMAND ----------

pser = pd.Series(np.random.randn(1000),
                 index=pd.date_range('1/1/2000', periods=1000))

# COMMAND ----------

kser = ks.Series(pser)

# COMMAND ----------

kser = kser.cummax()

# COMMAND ----------

kser.plot()

# COMMAND ----------

# MAGIC %md
# MAGIC On a DataFrame, the <a href="https://koalas.readthedocs.io/en/latest/reference/api/databricks.koalas.frame.DataFrame.plot.html#databricks.koalas.frame.DataFrame.plot">plot()</a> method is a convenience to plot all of the columns with labels:

# COMMAND ----------

pdf = pd.DataFrame(np.random.randn(1000, 4), index=pser.index,
                   columns=['A', 'B', 'C', 'D'])

# COMMAND ----------

kdf = ks.from_pandas(pdf)

# COMMAND ----------

kdf = kdf.cummax()

# COMMAND ----------

kdf.plot()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Getting data in/out
# MAGIC See the <a href="https://koalas.readthedocs.io/en/latest/reference/io.html">Input/Output
# MAGIC </a> docs.

# COMMAND ----------

# MAGIC %md
# MAGIC ### CSV
# MAGIC 
# MAGIC CSV is straightforward and easy to use. See <a href="https://koalas.readthedocs.io/en/latest/reference/api/databricks.koalas.DataFrame.to_csv.html#databricks.koalas.DataFrame.to_csv">here</a> to write a CSV file and <a href="https://koalas.readthedocs.io/en/latest/reference/api/databricks.koalas.read_csv.html#databricks.koalas.read_csv">here</a> to read a CSV file.

# COMMAND ----------

kdf.to_csv('/dbfs/tmp/foo.csv')
ks.read_csv('/dbfs/tmp/foo.csv').head(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parquet
# MAGIC 
# MAGIC Parquet is an efficient and compact file format to read and write faster. See <a href="https://koalas.readthedocs.io/en/latest/reference/api/databricks.koalas.DataFrame.to_parquet.html#databricks.koalas.DataFrame.to_parquet">here</a> to write a Parquet file and <a href="https://koalas.readthedocs.io/en/latest/reference/api/databricks.koalas.read_parquet.html#databricks.koalas.read_parquet">here</a> to read a Parquet file.

# COMMAND ----------

kdf.to_parquet('/dbfs/tmp/bar.parquet')
ks.read_parquet('/dbfs/tmp/bar.parquet').head(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark IO
# MAGIC 
# MAGIC In addition, Koalas fully support Spark's various datasources such as ORC and an external datasource.  See <a href="https://koalas.readthedocs.io/en/latest/reference/api/databricks.koalas.DataFrame.to_spark_io.html#databricks.koalas.DataFrame.to_spark_io">here</a> to write it to the specified datasource and <a href="https://koalas.readthedocs.io/en/latest/reference/api/databricks.koalas.read_spark_io.html#databricks.koalas.read_spark_io">here</a> to read it from the datasource.

# COMMAND ----------

kdf.to_spark_io('/dbfs/tmp/zoo.orc', format="orc")
ks.read_spark_io('/dbfs/tmp/zoo.orc', format="orc").head(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### And of course full support for Delta Lake

# COMMAND ----------

kdf.to_delta('/dbfs/tmp/baz.delta')
ks.read_delta('/dbfs/tmp/baz.delta').head(10)

# COMMAND ----------


