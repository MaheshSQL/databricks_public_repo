-- Databricks notebook source
-- MAGIC %md
-- MAGIC # EXTERNAL LOCATION AND CREDENTIALS
-- MAGIC 
-- MAGIC https://docs.databricks.com/data-governance/unity-catalog/manage-external-locations-and-credentials.html#language-Data%C2%A0Explorer 
-- MAGIC 


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create an External Table using External Location and Storage Credentials
-- MAGIC 
-- MAGIC Unity Catalog’s security and governance model provides excellent support for External Tables.  We will use the REST API to create a Storage Credential, and SQL to create an External Location and External Tables.

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- Now, you will create your own external location using your  storage credential. Run the following command. Note that this will create all the directories specified as location path. 
-- MAGIC --Use the storage credential you created that has the ARN that will connect to the URL where the data is
-- MAGIC -- Note the use of backticks around the <your_location_name> and <credential name>
-- MAGIC -- Either single or double quotes can be used around <your_location_path>
-- MAGIC 
-- MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `uc_demo_ext_tables` URL 's3://databricks-gsethi' WITH (STORAGE CREDENTIAL `e2-uc-passrole`); 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- grant your users ( e.g. data engineers) permission to create tables using the CREATE TABLE permission on the external location
-- MAGIC -- To be able to navigate and list data in the external cloud storage, users also need READ FILES permission.
-- MAGIC -- For the purpose of this quickstart, we are granting permissions to all users, i.e. account users
-- MAGIC 
-- MAGIC -- Note the use of backticks around the <your_location_name> and <account users>
-- MAGIC 
-- MAGIC GRANT CREATE TABLE, READ FILES 
-- MAGIC ON EXTERNAL LOCATION `databricks-gsethi`
-- MAGIC TO `account users`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **SWITCH CONTEXT TO CATALOG AND DATABASE**

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC USE CATALOG `uc-demo-tpch`;
-- MAGIC USE tpch;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC --- list existing storage credentials
-- MAGIC 
-- MAGIC SHOW STORAGE CREDENTIALS;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SHOW EXTERNAL LOCATIONS

-- COMMAND ----------

-- Note: either single quotes or double-quotes can be used around <your_location_path>, but not backticks

LIST 's3://databricks-gsethi'

-- COMMAND ----------

--- create a new schema for ext tables
CREATE SCHEMA IF NOT EXISTS `uc-demo-tpch`.external_loc;

-- COMMAND ----------

--- create an external table using CREATE TABLE query, pointing the location to the directory from above.
CREATE TABLE IF NOT EXISTS `uc-demo-tpch`.external_loc.Country
(
Country_ID INT,
Country_NAME String,
Region_ID INT

)
USING Delta
LOCATION 's3://databricks-gsethi/country/'

-- COMMAND ----------


SELECT * FROM `uc-demo-tpch`.external_loc.Country;

-- COMMAND ----------

-- Note: either single quotes or double-quotes can be used around <your_location_path>, but not backticks

LIST 's3://databricks-gsethi/'

-- COMMAND ----------

INSERT INTO `uc-demo-tpch`.external_loc.Country VALUES
(1,'South Africa',0),
(2,'Kenya',0),
(3,'United States Of America',1),
(4,'Canda',1),
(5,'Singapore',2),
(6,'India',2),
(7,'United Kingdom',3),
(8,'France',3),
(9,'Germany',3),
(10,'United Arab Emirates',4);

-- COMMAND ----------

SELECT * FROM external_loc.Country;

-- COMMAND ----------

SELECT * FROM REGION

-- COMMAND ----------

SELECT R.R_Name as Region_Name,C.Country_Name FROM Region R JOIN external_loc.country C ON R.R_RegionKey=C.Region_ID;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **INSERT NEW RECORDS AND VERIFY IF ABLE TO SEE THIS CHANGE IN DELTA SHARING**

-- COMMAND ----------

INSERT INTO `uc-demo-tpch`.external_loc.Country VALUES
(11,'Ghana',0)

-- COMMAND ----------

INSERT INTO `uc-demo-tpch`.external_loc.Country VALUES
(14,'Australia',0)
