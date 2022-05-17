# Databricks notebook source
# Connect With Storage Via Azure Key Vault Secret
configs={
    "fs.azure.account.auth.type": "CustomAccessToken",
    "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# COMMAND ----------

##########################
#  Procedure to Unmount  # 
##########################

def DatalakeUnmount(datalakezone):
  #Define a Mount Point.
  MOUNT_POINT = "/mnt/datalake-" + datalakezone
  print ("Unmounting " + MOUNT_POINT)
  dbutils.fs.unmount(MOUNT_POINT)

# COMMAND ----------


##########################
#  Procedure to Mount    # 
##########################

def DatalakeMount(datalakezone):
  print ("Starting : DataLakeGetMountPoint for zone : " + datalakezone)

  #Define a Mount Point.
  MOUNT_POINT = "/mnt/datalake-" + datalakezone


  # Scope is saved in Key vault 
  STORAGE_ACCOUNT_CREDENTIALS = dbutils.secrets.get(scope="gsethi-kv-scope",key="gsethi-storage-secret")
  
  #Create the path for the Data Lake along with the Container
  DATA_LAKE_SOURCE = "wasbs://{container}@gsethistorage.blob.core.windows.net/".format(container = datalakezone, datalakeaccount = STORAGE_ACCOUNT_CREDENTIALS)

  print("Path : {} | MountPoint : {} | Data Lake : {}".format(DATA_LAKE_SOURCE, MOUNT_POINT, STORAGE_ACCOUNT_CREDENTIALS))
  
  try:
    fileList = dbutils.fs.ls(MOUNT_POINT)
    return MOUNT_POINT
  except Exception as e:
    print("Directory not already mounted.")
  
  
  print("Starting to mount...")
  try:
    dbutils.fs.mount(
      source = DATA_LAKE_SOURCE,
      mount_point = MOUNT_POINT,
      extra_configs = {"fs.azure.account.key.gsethistorage.blob.core.windows.net":STORAGE_ACCOUNT_CREDENTIALS})  
    print("{0} Directory Mounted".format(MOUNT_POINT))
    print(dbutils.fs.ls(MOUNT_POINT))
    return MOUNT_POINT
  except Exception as e:
    if "Directory already mounted" in str(e):
      #Ignore error if the container is already mounted
      print("{0} already mounted".format(MOUNT_POINT))
      return MOUNT_POINT
    else:
      #Else raise a error. Something's gone wrong
      raise e

# COMMAND ----------

# UNMOUNT 
DatalakeUnmount("raw")
DatalakeUnmount("silver")
DatalakeUnmount("gold")


# COMMAND ----------

# MOUNT STORAGE
DatalakeMount("raw")
DatalakeMount("silver")
DatalakeMount("gold")


# COMMAND ----------

# Validate If Mountpoint datalake-raw exists
dbutils.fs.ls("/mnt/datalake-raw")


# COMMAND ----------

# Validate If Mountpoint datalake-silver exists
dbutils.fs.ls("/mnt/datalake-silver")


# COMMAND ----------

# Validate If Mountpoint datalake-gold exists
dbutils.fs.ls("/mnt/datalake-gold")

# COMMAND ----------

  #Get Secrets from the Key Vault using the databricks defined scope
  #CLIENT_ID = dbutils.secrets.get(scope = "dgit-npe-ase-ADBWS-Secret-Scope", key = "dgitnpeaseadls-sp-client-id")
  #CLIENT_KEY = dbutils.secrets.get(scope = "dgit-npe-ase-ADBWS-Secret-Scope", key = "dgitnpeaseadls-sp-secret-value")
  #DIRECTORY_ID = dbutils.secrets.get(scope = "dgit-npe-ase-ADBWS-Secret-Scope", key = "dgitnpeaseadls-sp-tenant-id")
  
  #Standard Configurations for connecting to Data Lake
  #configs = {"fs.azure.account.auth.type": "OAuth",
  #         "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  #         "fs.azure.account.oauth2.client.id": CLIENT_ID,
  #         "fs.azure.account.oauth2.client.secret": CLIENT_KEY,
  #         "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/{0}/oauth2/token".format(DIRECTORY_ID)}
