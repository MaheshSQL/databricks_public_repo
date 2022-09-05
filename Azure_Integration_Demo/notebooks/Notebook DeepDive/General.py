# Databricks notebook source
# MAGIC %md <a href='$../Azure Integrations Start Here'>Home</a>

# COMMAND ----------

# MAGIC %md
# MAGIC # General Use

# COMMAND ----------

# MAGIC %md
# MAGIC ### Annotations
# MAGIC 
# MAGIC use the %md magic command for denoting a cell as a MarkDown cell. Headings can be created with #

# COMMAND ----------

# MAGIC %md
# MAGIC ### Images can be embedded with regular HTML tags, eg:
# MAGIC 
# MAGIC <img src='https://gsethistorageaccount.blob.core.windows.net/images/Jup_214407_pipp_lapl5_ap27-wavelet.jpg' width=350>
# MAGIC <img src='https://gsethistorageaccount.blob.core.windows.net/images/Saturn_223110_pipp_lapl5_ap43-wavelet.jpg' width=350>
# MAGIC <img src='https://gsethistorageaccount.blob.core.windows.net/images/Mars_015524_pipp_lapl5_ap17-wavelet.jpg' width=350>
# MAGIC <img src='https://gsethistorageaccount.blob.core.windows.net/images/Neptune_014046_pipp_lapl5_ap5-wavelet.jpg' width=350>
# MAGIC 
# MAGIC You can also link to files in the DBFS FileStore using (but I don't have an image there so it's broken!):
# MAGIC 
# MAGIC ![myImage](files/myImage.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC ### LaTeK Support for mathematical equations and formulas
# MAGIC 
# MAGIC \\(c = \\pm\\sqrt{a^2 + b^2} \\)
# MAGIC 
# MAGIC \\(A{_i}{_j}=B{_i}{_j}\\)
# MAGIC 
# MAGIC $$c = \\pm\\sqrt{a^2 + b^2}$$
# MAGIC 
# MAGIC \\[A{_i}{_j}=B{_i}{_j}\\]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Switch between languages with the magic commands:
# MAGIC <ul>
# MAGIC <li>Python - %python
# MAGIC   <li>SQL - %sql
# MAGIC     <li>Scala - %scala
# MAGIC       <li>R - %R
# MAGIC </ul>

# COMMAND ----------

print('hello')

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC print('Hello this is Python!')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 'Hello this is SQL!' AS Comment

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC print("Hello this is scala");

# COMMAND ----------

# MAGIC %r
# MAGIC 
# MAGIC print("And this is R")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Linking to other notebooks using the href tag:
# MAGIC 
# MAGIC <a href='$./Git Integration'>Git Integration</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Help available by calling the help() method for the various dbutils libraries

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -l /dbfs

# COMMAND ----------


