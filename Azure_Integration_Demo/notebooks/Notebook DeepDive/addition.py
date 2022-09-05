# Databricks notebook source
# take the 2 passed in parameters and add them
dbutils.widgets.text('X', '0')
dbutils.widgets.text('Y', '0')
X = dbutils.widgets.get('X')
Y = dbutils.widgets.get('Y')

x = int(X)
y = int(Y)

dbutils.notebook.exit(x + y)

# COMMAND ----------


