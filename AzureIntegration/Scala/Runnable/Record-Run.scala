// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Parameterizing Notebooks
// MAGIC 
// MAGIC The [Databricks Utilities module](https://docs.databricks.com/dev-tools/databricks-utils.html) includes a number of methods to make notebooks more extensible and easier to take to production. This notebook is designed to be scheduled as a job, but can also be run interactively.
// MAGIC 
// MAGIC ### Learning Objectives
// MAGIC - Pass parameters to notebooks using widgets
// MAGIC - Return values from notebooks using exit value

// COMMAND ----------

// MAGIC %md
// MAGIC ## Widgets
// MAGIC 
// MAGIC The `widgets` submodule includes a number of methods to allow interactive variables to be set while working with notebooks in the workspace with an interactive cluster. To learn more about this functionality, refer to the [Databricks documentation](https://docs.databricks.com/notebooks/widgets.html#widgets).
// MAGIC 
// MAGIC This notebook will focus on only two of these methods, emphasizing their utility when running a notebook as a job:
// MAGIC 1. `dbutils.widgets.text` accepts a parameter name and a default value. This is the method through which external values can be passed into scheduled notebooks.
// MAGIC 1. `dbutils.widgets.get` accepts a parameter name and retrieves the associated value from the widget with that parameter name.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC In the cell below, a text widget is created with the default value `"notebook"`. This widget expects values to be passed as strings.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> If you run this cell in an interactive notebook, you will see the widget populated with the default value at the top of the notebook. This can be manually manipulated.

// COMMAND ----------

// MAGIC %scala
// MAGIC dbutils.widgets.text("ranBy", "notebook")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC The cell below retrieves the value currently associated with the widget and assigns it to a variable. Remember that this value will be passed as a string--be sure to cast it to the correct type if you wish to pass numeric values or use JSON to pass multiple fields.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> If no parameter is passed to the notebook when scheduling, the default value will be used.

// COMMAND ----------

// MAGIC %scala
// MAGIC val ranBy = dbutils.widgets.get("ranBy")

// COMMAND ----------

// MAGIC %md
// MAGIC Taken together, `dbutils.widgets.text` allows the passing of external values and `dbutils.widgets.get` allows those values to be referenced.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Parameterized Logic
// MAGIC The following code block writes a simple file that records the time the notebook was run and the value associated with the `"ranBy"` parameter/widget. The final line displays the full content of this file from all previous executions by the present user.

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.functions.{lit, unix_timestamp}
// MAGIC import org.apache.spark.sql.types.TimestampType
// MAGIC 
// MAGIC val tags = com.databricks.logging.AttributionContext.current.tags
// MAGIC val username = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
// MAGIC val path = "/users/"+username+"/runLog"
// MAGIC 
// MAGIC spark
// MAGIC   .range(1)
// MAGIC   .select(unix_timestamp.alias("runtime").cast(TimestampType), lit(ranBy).alias("ranBy"))
// MAGIC   .write
// MAGIC   .mode("APPEND")
// MAGIC   .parquet(path)
// MAGIC 
// MAGIC display(spark.read.parquet(path))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exit Value
// MAGIC The `notebook` submodule contains only two methods. [Documentation here](https://docs.databricks.com/notebooks/notebook-workflows.html#notebook-workflows).
// MAGIC 1. `dbutils.notebook.run` allows you to call another notebook using a relative path.
// MAGIC 1. `dbutils.notebook.exit` allows you to return an exit value that can be captured and referenced by integrated scheduling services and APIs. While running in interactive mode, this is essentially a no-op as this value does not go anywhere.

// COMMAND ----------

// MAGIC %md
// MAGIC In the cell below, the value associated with the variable `path` is returned as the exit value.

// COMMAND ----------

// MAGIC %scala
// MAGIC dbutils.notebook.exit(path)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>