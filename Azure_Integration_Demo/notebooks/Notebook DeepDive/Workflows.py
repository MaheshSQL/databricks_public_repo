# Databricks notebook source
# MAGIC %md <a href='$../Azure Integrations Start Here'>Home</a>

# COMMAND ----------

# MAGIC %md
# MAGIC # Workflows

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parent/Child Notebooks with the %run command

# COMMAND ----------

# MAGIC %run ./children/child1

# COMMAND ----------

child1

# COMMAND ----------

# MAGIC %run ./children/child2

# COMMAND ----------

child2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using Notebook Workflows

# COMMAND ----------

x = 4
y = 2

z = dbutils.notebook.run('addition', 60, {'X': str(x), 'Y': str(y)})
print(z)
