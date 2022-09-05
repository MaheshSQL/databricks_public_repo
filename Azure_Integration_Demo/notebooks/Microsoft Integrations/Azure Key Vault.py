# Databricks notebook source
# MAGIC %md <a href='$../Azure Integrations Start Here'>Home</a>

# COMMAND ----------

# MAGIC %md
# MAGIC # Azure Key Vault

# COMMAND ----------

# MAGIC %md
# MAGIC To use Azure Key Vault from within Databricks, you need to do the following things first:
# MAGIC 
# MAGIC 1. Create the Key Vault from the Azure Portal
# MAGIC 2. Create the AKV backed secret scope in the Databricks Workspace
# MAGIC 3. Create your secret in AKV

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1
# MAGIC Login to the Azure portal and create a new Azure Key Vault in an appropriate resource group. The key vault does not need to exist in the same resource group as the Databricks workspace.
# MAGIC <br></br>
# MAGIC 
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 10; padding-top: 30px;  padding-bottom: 30px;">
# MAGIC   <img src="https://gsethistorageaccount.blob.core.windows.net/images/Create_KeyVault.jpg" alt='GrabNGoInfo Logo' style="width: 50px" >
# MAGIC </div>
# MAGIC 
# MAGIC <br></br>
# MAGIC <br></br>
# MAGIC 
# MAGIC 
# MAGIC ### Step 2
# MAGIC From the Databricks workspace, use the following URL to create a new secret scope. The URL is:
# MAGIC 
# MAGIC https://{databricks-instance}#secrets/createScope
# MAGIC 
# MAGIC with the workspace ID similar to: https://adb-382266861287659.19.azuredatabricks.net/?o=382266861287659#secrets/createScope
# MAGIC 
# MAGIC <br></br>
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 10; padding-top: 30px;  padding-bottom: 30px;">
# MAGIC   <img src="https://jokdemoresourcessa.blob.core.windows.net/images/adbsecscope.png" alt='GrabNGoInfo Logo' style="width: 50px" >
# MAGIC </div>
# MAGIC 
# MAGIC <br></br>
# MAGIC <br></br>
# MAGIC 
# MAGIC You will need the parameters from the Azure Key Vault properties page to fill in items 1 and 2.
# MAGIC 
# MAGIC <br></br>
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 10; padding-top: 30px;  padding-bottom: 30px;">
# MAGIC   <img src="https://gsethistorageaccount.blob.core.windows.net/images/KeyVault_Properties.jpg" alt='GrabNGoInfo Logo' style="width: 50px" >
# MAGIC </div>
# MAGIC 
# MAGIC <br></br>
# MAGIC <br></br>
# MAGIC 
# MAGIC ### Step 3
# MAGIC From the Azure Key Vault portal page, create the secret to be used.
# MAGIC 
# MAGIC <br></br>
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 10; padding-top: 30px;  padding-bottom: 30px;">
# MAGIC   <img src="https://gsethistorageaccount.blob.core.windows.net/images/KeyVault.jpg" alt='GrabNGoInfo Logo' style="width: 50px" >
# MAGIC </div>
# MAGIC 
# MAGIC <br></br>
# MAGIC <br></br>
# MAGIC For more information setting up the the Azure Key Vault backed secret scope, see the following documentation:
# MAGIC 
# MAGIC https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes#create-an-azure-key-vault-backed-secret-scope-using-the-ui

# COMMAND ----------

# MAGIC %md
# MAGIC Once the secret has been created, you can access the value using the dbutils.secrets.get() method call.

# COMMAND ----------

akv_secret_scope = "gsethi-kv-scope" # The Azure Key Vault Secret Scope
akv_secret_key = "gsethi-storage-secret" # The AKV secret key name corresponding to the secret

mysecret =  dbutils.secrets.get(scope = akv_secret_scope, key = akv_secret_key)
print(mysecret)

# COMMAND ----------

# MAGIC %md
# MAGIC Note that the secret will be redacted from any output.

# COMMAND ----------

# MAGIC %md
# MAGIC Non-Existance of any Secret in Key-Vault will result in errors like below.

# COMMAND ----------

akv_secret_scope = "gsethi-kv-scope" # The Azure Key Vault Secret Scope
akv_secret_key = "gsethi-storage-secret2" # The AKV secret key name corresponding to the secret

mysecret =  dbutils.secrets.get(scope = akv_secret_scope, key = akv_secret_key)
print(mysecret)
