# Databricks notebook source
# MAGIC %md <a href='$../Azure Integrations Start Here'>Home</a>

# COMMAND ----------

# MAGIC %md
# MAGIC # Azure Storage

# COMMAND ----------

# MAGIC %md ## Azure Blob Storage

# COMMAND ----------

# MAGIC %md #### Using a Storage Account Key (SAS)

# COMMAND ----------

dbutils.widgets.text('My Storge Access Key', '')

# COMMAND ----------

# Secret! - Access keys hidden for security!

storage_account_name = "gsethistorageaccount"
storage_container = "datasets"
storage_account_access_key = dbutils.widgets.get('My Storge Access Key') 
file_location = "wasbs://" + storage_container + "@" + storage_account_name + ".blob.core.windows.net/"
file_type = "json"
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

filename = 'simple-GSETHI-A-2.f6.json'
df = spark.read.format(file_type).option("inferSchema", "true").load(file_location + filename)
df.head(5)

# COMMAND ----------

# MAGIC %md #### Using a secret stored in Azure Key Vault

# COMMAND ----------

# MAGIC %md <img src="https://gsethistorageaccount.blob.core.windows.net/images/KeyVault.jpg" width="800">

# COMMAND ----------

akv_secret_scope = "gsethi-kv-scope" # The Azure Key Vault Secret Scope
akv_secret_key = "gsethi-storage-secret" # The AKV secret key name corresponding to the secret
storage_account_name = "gsethistorageaccount"
storage_container = "datasets"
file_location = "wasbs://" + storage_container + "@" + storage_account_name + ".blob.core.windows.net/"
file_type = "json"
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  dbutils.secrets.get(scope = akv_secret_scope, key = akv_secret_key))

filename = 'simple-GSETHI-A-2.f6.json'
df = spark.read.format(file_type).option("inferSchema", "true").load(file_location + filename)
df.head(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using a Shared Access Signature (SAS) Directly

# COMMAND ----------

dbutils.widgets.text('Shared Access Signature URL', '')

dbutils.widgets.text('Shared Access Signature Token', '')

# COMMAND ----------

storage_account_name = "gsethistorageaccount"
storage_container = "datasets"
# Seems either sas_blob or sas will work
sas_blob = dbutils.widgets.get('Shared Access Signature URL')  
sas = dbutils.widgets.get('Shared Access Signature Token')
file_location = "wasbs://" + storage_container + "@" + storage_account_name + ".blob.core.windows.net/"
file_type = "json"
spark.conf.set(
  "fs.azure.sas."+storage_container+"."+storage_account_name+".blob.core.windows.net",
  sas_blob)

filename = 'simple-GSETHI-A-2.f6.json'
df = spark.read.format(file_type).option("inferSchema", "true").load(file_location + filename)
df.head(5)

# COMMAND ----------

# MAGIC %md #### Mounting a Blob Storage Account

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://datasets@gsethistorageaccount.blob.core.windows.net/",
  mount_point = "/mnt/datasets",
  extra_configs = {"fs.azure.account.key.gsethistorageaccount.blob.core.windows.net":"gsethistorageaccount"})

# COMMAND ----------

# MAGIC %sh ls -l /dbfs/mnt/datasets

# COMMAND ----------

# Unmount the storage account
dbutils.fs.unmount('/mnt/datasets')

# COMMAND ----------

# MAGIC %md ## Azure Data Lake Storage Generation Two (ADLSg2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Direct access via Storage Account Key

# COMMAND ----------

# Set up direct account access key

adlsname = 'gsethistorageaccount'
adlscontainer = 'datasets'
adlsloc = 'abfss://' + adlscontainer + '@' + adlsname + '.dfs.core.windows.net/'

# COMMAND ----------

spark.conf.set(
  'fs.azure.account.key.' + adlsname + '.dfs.core.windows.net',
  dbutils.widgets.get('My Storge Access Key')
)

# COMMAND ----------

df = spark.read.load(adlsloc + 'scalartelem', format='delta')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

df.createOrReplaceTempView('ksptelem')

# COMMAND ----------

# MAGIC %sql
# MAGIC select avg(alt), utime from ksptelem group by utime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mounting an ADLSg2 Storage Container on a DBFS mount point
# MAGIC ### First example uses AAD Credential Passthrough
# MAGIC ##### Which means the cluster must have credential passthrough enabled or it will fail.
# MAGIC ##### Also - Note that the user credential needs to have the "Storage Blob Data Contributor" AD role on the container to be able to read the data - being Owner is not enough! Owner only gives you rights to manage the container, but not access the contents of the container. Also applicable to SP below.

# COMMAND ----------

# Unmount if already mounted
dbutils.fs.unmount('/mnt/datasets')

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class":   spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}
dbutils.fs.mount(
  source = "abfss://datasets@gsethistorageaccount.dfs.core.windows.net/",
  mount_point = "/mnt/datasets",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls('/mnt/datasets')

# COMMAND ----------

dbutils.fs.unmount('/mnt/datasets')

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/mnt/datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ### This next example uses Azure AD Service Principals
# MAGIC ##### Note that the SP needs to have the "Storage Blob Data Contributor" AD role on the container to be able to read the data - being Owner is not enough! Owner only gives you rights to manage the container, but not access the contents of the container.

# COMMAND ----------

# MAGIC %md
# MAGIC <pre>
# MAGIC configs = {"fs.azure.account.auth.type": "OAuth",
# MAGIC            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# MAGIC            "fs.azure.account.oauth2.client.id": "[application-id]",
# MAGIC            "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="[scope-name]",key="[service-credential-key-name]"),
# MAGIC            "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/[directory-id]/oauth2/token"}
# MAGIC 
# MAGIC dbutils.fs.mount(
# MAGIC   source = "abfss://deltalake@jokdemoresourcesadls.dfs.core.windows.net/",
# MAGIC   mount_point = "/mnt/deltalake",
# MAGIC   extra_configs = configs)
# MAGIC </pre>

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="gsethi-kv-scope",key="Azure-App-Registration-ClientID"),
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="gsethi-kv-scope",key="Azure-App-Registration-Client-Secret"),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/9f37a392-f0ae-4280-9796-f1864a10effc/oauth2/token"}

dbutils.fs.mount(
  source = "abfss://datasets@gsethistorageaccount.dfs.core.windows.net/",
  mount_point = "/mnt/datasets",
  extra_configs = configs)


# COMMAND ----------

dbutils.fs.ls('/mnt/datasets')

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -l /dbfs/mnt/datasets

# COMMAND ----------

# MAGIC %sh
# MAGIC head /dbfs/mnt/datasets/simple-GSETHI-A-2.f6.json

# COMMAND ----------

dbutils.fs.unmount('/mnt/datasets')

# COMMAND ----------

# MAGIC %md
# MAGIC Remove Widgets

# COMMAND ----------

dbutils.widgets.remove('My Storge Access Key')

dbutils.widgets.remove('Shared Access Signature URL')

dbutils.widgets.remove('Shared Access Signature Token')
