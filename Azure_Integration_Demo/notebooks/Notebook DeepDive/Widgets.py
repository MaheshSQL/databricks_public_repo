# Databricks notebook source
# MAGIC %md <a href='$../Azure Integrations Start Here'>Home</a>

# COMMAND ----------

# MAGIC %md
# MAGIC # Widgets

# COMMAND ----------

# MAGIC %md
# MAGIC ### Text Box

# COMMAND ----------

dbutils.widgets.text('My Text Box', 'Hello')

# COMMAND ----------

# What is in it?
print(dbutils.widgets.get('My Text Box'))

# COMMAND ----------

dbutils.widgets.remove('My Text Box')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dropdown

# COMMAND ----------

dbutils.widgets.dropdown('X', '1', [str(x) for x in range(1, 10)])

# COMMAND ----------

dbutils.widgets.get('X')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Combobox

# COMMAND ----------

dbutils.widgets.combobox('MyCombo', 'Colours', ['Red', 'Green', 'Blue'])

# COMMAND ----------

dbutils.widgets.get('MyCombo')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Multiselect

# COMMAND ----------

dbutils.widgets.multiselect('MultiselectDemo', 'Large', ['Large', 'Extra Large', 'Ridiculous'])

# COMMAND ----------

print(dbutils.widgets.get('MultiselectDemo'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL Widgets

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT sqlWidget DEFAULT 'Put something here!'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT getArgument('sqlWidget') AS sqlWidget

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup

# COMMAND ----------

dbutils.notebook.exit("stop") 

# COMMAND ----------

dbutils.widgets.remove('X')
dbutils.widgets.remove('MyCombo')
dbutils.widgets.remove('MultiselectDemo')
dbutils.widgets.remove('sqlWidget')
