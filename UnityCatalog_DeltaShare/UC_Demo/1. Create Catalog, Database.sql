-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Unity Catalog Quickstart (SQL)
-- MAGIC 
-- MAGIC A notebook that provides an example workflow for using the Unity Catalog. Steps include:
-- MAGIC 
-- MAGIC - Creating a catalog and granting permissions
-- MAGIC - Creating a new schema aka database and granting permissions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Widgets to get input parameters

-- COMMAND ----------

CREATE WIDGET text CatalogName DEFAULT "`uc-demo-tpch`";
CREATE WIDGET text SchemaName DEFAULT "tpch";
CREATE WIDGET text AdminUser DEFAULT "";
CREATE WIDGET text AmeaUser DEFAULT "";
CREATE WIDGET text EmeaUser DEFAULT "";


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Namespace
-- MAGIC 
-- MAGIC In the previous setup notebook you created a metastore catalog. Within a metastore, Unity Catalog provides a three-level namespace for organizing data: catalogs, schemas (also called databases), and tables / views
-- MAGIC 
-- MAGIC >`Catalog.Schema.Table`
-- MAGIC 
-- MAGIC For Databricks users who already use the Apache Hive metastore available in each workspace, or an external Hive metastore, Unity Catalog is **additive**: the workspace’s Hive metastore becomes one catalog within the 3-layer namespace (called “legacy”), and other catalogs use Unity Catalog.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create New Catalog
-- MAGIC 
-- MAGIC After the initial setup of Unity Catalog a default catalog named 'main' will be created along with an empty schema 'default'. 
-- MAGIC 
-- MAGIC New catalogs can be created using the `CREATE CATALOG` command.

-- COMMAND ----------

--CREATE A NEW CATALOG

CREATE CATALOG IF NOT EXISTS $Catalog_Name;

-- COMMAND ----------

SHOW CATALOGS

-- COMMAND ----------

--CHECK THAT THE GRANTS ARE CORRECT ON THE uc-demo-tpch Catalog

--SHOW GRANT ON CATALOG `uc-demo-tpch`;

SHOW GRANT ON CATALOG $CatalogName;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create and View Schemas
-- MAGIC Schemas, also referred to as Databases, are the second layer of the Unity Catalog namespace. They can be used to logically organize tables. 

-- COMMAND ----------

--SWITCH Context to selected/defined Catalog

USE CATALOG $CatalogName;

--CREATE a New Schema/Database under selected Catalog

CREATE SCHEMA IF NOT EXISTS $SchemaName COMMENT "A New Unity Catakig Schema Called tpch"



-- COMMAND ----------

--SHOW Schames Under Catalog

SHOW SCHEMAS

-- COMMAND ----------

--DESCRIBE THE NEW SCHEMA

DESCRIBE SCHEMA EXTENDED $SchemaName;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Grant Permissions on 
-- MAGIC Schemas, also referred to as Databases, are the second layer of the Unity Catalog namespace. They can be used to logically organize tables. 

-- COMMAND ----------

--GRANT CREATE AND USAGE PERMISSIONS to respective Groups/Users

GRANT USAGE,CREATE ON DATABASE $CatalogName.$SchemaName TO $AdminUser;
GRANT USAGE, CREATE ON CATALOG $CatalogName to $AmeaUser;
GRANT USAGE, CREATE ON CATALOG $CatalogName to $EmeaUser;

-- COMMAND ----------

--Grant SELECT and MODIFY access to Tables

--GRANT SELECT ON TABLE `uc-demo-tpch`.tpch.F_LINEITEM_RESTRICTED TO `gsethi-uc-demo-admin`;
--GRANT SELECT ON TABLE `uc-demo-tpch`.tpch.D_ORDER_RESTRICTED TO `gsethi-uc-demo-admin`;
--GRANT SELECT ON TABLE `uc-demo-tpch`.tpch.D_CUSTOMER_RESTRICTED TO `gsethi-uc-demo-admin`;
--GRANT SELECT, MODIFY ON TABLE `uc-demo-tpch`.tpch.F_LINEITEM TO `gsethi-uc-demo-admin`;
--GRANT SELECT, MODIFY ON TABLE `uc-demo-tpch`.tpch.D_ORDER TO `gsethi-uc-demo-admin`;
--GRANT SELECT, MODIFY ON TABLE `uc-demo-tpch`.tpch.D_CUSTOMER TO `gsethi-uc-demo-admin`;
--GRANT SELECT, MODIFY ON TABLE `uc-demo-tpch`.tpch.D_PART TO `gsethi-uc-demo-admin`;
--GRANT SELECT, MODIFY ON TABLE `uc-demo-tpch`.tpch.D_SUPPLIER TO `gsethi-uc-demo-admin`;

--GRANT SELECT, MODIFY ON TABLE `uc-demo-tpch`.tpch.region TO `gsethi-uc-demo-admin`;
--GRANT SELECT, MODIFY ON TABLE `uc-demo-tpch`.tpch.customer TO `gsethi-uc-demo-admin`;
--GRANT SELECT, MODIFY ON TABLE `uc-demo-tpch`.tpch.lineitem TO `gsethi-uc-demo-admin`;
--GRANT SELECT, MODIFY ON TABLE `uc-demo-tpch`.tpch.nation TO `gsethi-uc-demo-admin`;
--GRANT SELECT, MODIFY ON TABLE `uc-demo-tpch`.tpch.orders TO `gsethi-uc-demo-admin`;
--GRANT SELECT, MODIFY ON TABLE `uc-demo-tpch`.tpch.part TO `gsethi-uc-demo-admin`;
--GRANT SELECT, MODIFY ON TABLE `uc-demo-tpch`.tpch.partsupp TO `gsethi-uc-demo-admin`;
--GRANT SELECT, MODIFY ON TABLE `uc-demo-tpch`.tpch.supplier TO `gsethi-uc-demo-admin`;
--GRANT SELECT, MODIFY ON TABLE `uc-demo-tpch`.tpch.tableOptimizations TO `gsethi-uc-demo-admin`;


