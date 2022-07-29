-- Databricks notebook source
-- MAGIC %md
-- MAGIC **Widget to procide Catalog, Schema/Database Name**

-- COMMAND ----------

CREATE WIDGET text CatalogName DEFAULT "`uc-demo-tpch`";
CREATE WIDGET text SchemaName DEFAULT "tpch";


-- COMMAND ----------

USE CATALOG $CatalogName;
USE $SchemaName;

-- COMMAND ----------

--List all tables in tpch database 

SHOW TABLES

-- COMMAND ----------

--SHOW EXTENDED Properties of a VIEW which is created based on a SELECT statement on a base table.
--We can see the underlying QUERY as well of the view

--BOTH AMEA & EMEA users will be able to run this command

DESCRIBE TABLE EXTENDED vw_DIM_CUSTOMER

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **current_user():** returns the current user name.
-- MAGIC 
-- MAGIC **is_account_group_member():** determines if the current user is a member of a specific account-level group. Recommended for use in Unity Catalog.
-- MAGIC 
-- MAGIC **is_member():** determines if the current user is a member of a specific workspace-level group. This function is provided for compatibility with the existing Hive Metastore, though should be avoided when used with Unity Catalog as it refers to group definitions within individual workspaces.

-- COMMAND ----------

--IS_MEMBER() is a function to check if current logged in user is member of specified group or not

SELECT is_member('gsethi-uc-demo-amea'), is_member('gsethi-uc-demo-emea')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC **AMEA USER** - Will be able to SUCCESSFULLY RUN this query as **SECURITY MODE** for cluster is **USER ISOLATION**
-- MAGIC 
-- MAGIC **EMEA USER** - Will get an ERROR and **SECURITY MODE** for cluster is **SINGLE USER**
-- MAGIC 
-- MAGIC **Clusters in SINGLE USER - SECURITY MODE does not support checking permissions of the view owner at execution time. Users selecting from views on these clusters will execute the view query as themselves. This means that to be able to select from a view, users also require permissions on the tables and views referenced in its definition.**

-- COMMAND ----------

SELECT * FROM vw_FACT_LINEITEM

-- COMMAND ----------

select distinct TEAM_OWNER from vw_FACT_LINEITEM

-- COMMAND ----------

select * from DIM_CUSTOMER

-- COMMAND ----------

SELECT * FROM vw_DIM_CUSTOMER

-- COMMAND ----------

SELECT * FROM vw_DIM_ORDER

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC **AMEA USER** - Will be able to SUCCESSFULLY RUN this query as they are granted access on this base table.
-- MAGIC 
-- MAGIC **EMEA USER** - Will be able to SUCCESSFULLY RUN this query as they are granted access on this base table.

-- COMMAND ----------

select * from DIM_PART

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC **AMEA USER** - Will be able to SUCCESSFULLY RUN this query as they are granted access on this base table.
-- MAGIC 
-- MAGIC **EMEA USER** - Will be able to SUCCESSFULLY RUN this query as they are granted access on this base table.

-- COMMAND ----------

SELECT * FROM DIM_SUPPLIER
