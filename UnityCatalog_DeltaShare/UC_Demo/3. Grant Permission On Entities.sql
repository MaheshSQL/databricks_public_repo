-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Managing Permissions on Data
-- MAGIC 
-- MAGIC Data access control lets you grant and revoke access to your data. The Unity Catalog is a “shall grant” secure-by-default architecture. Initially, all users have no access to data.  Only administrators have access to data, and can grant/revoke access to users/groups on specific data assets.
-- MAGIC 
-- MAGIC #### Ownership
-- MAGIC Every securables in Unity Catalog has an owner. The owner can be any account-level user or group, called *principals* in general. A principal becomes the owner of a securable when they create it. Pricipals are also used to manage permissions to data in the metastore.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Input Widegts to specificy Catalog and Schema/Database Name and User Groups

-- COMMAND ----------

CREATE WIDGET text CatalogName DEFAULT "`uc-demo-tpch`";
CREATE WIDGET text SchemaName DEFAULT "tpch";
CREATE WIDGET text AmeaUserGroup DEFAULT "";
CREATE WIDGET text EmeaUserGroup DEFAULT "";


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Managing Privileges
-- MAGIC The following are the SQL privileges supported on Unity Catalog:
-- MAGIC - SELECT: Allows the grantee to read data from the securable.
-- MAGIC - MODIFY: Allows the grantee to add,  update and delete data to or from the securable.
-- MAGIC - CREATE: Allows the grantee to create child securables within this securable.
-- MAGIC - USAGE: Allows the grantee to read or modify (add/delete/update) the child securables within this securable.
-- MAGIC 
-- MAGIC Note that privileges are NOT inherited on the securables. This means granting a privilege on a securable DOES NOT automatically grant the privilege on its child securables.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Granting**: Grants a privilege on securable(s) to a principal. Only Metastore Admin and owners of the securable can perform privilege granting.

-- COMMAND ----------

--Swith Context to `uc-demo-tpch` Catalog

USE CATALOG $CatalogName;

-- COMMAND ----------

-- Grant USAGE permissions on Catalog to EMEA Group

GRANT USAGE ON CATALOG $CatalogName TO $EmeaUserGroup;

-- COMMAND ----------

-- Grant USAGE permissions on Schema/Database to EMEA Group

GRANT USAGE ON DATABASE $CatalogName.$SchemaName TO $EmeaUserGroup;


-- COMMAND ----------

-- Grant USAGE permissions on Catalog to AMEA Group

GRANT USAGE ON CATALOG $CatalogName TO $AmeaUserGroup;

-- COMMAND ----------

-- Grant USAGE permissions on Schema/Database to AMEA Group

GRANT USAGE ON DATABASE $CatalogName.$SchemaName TO $AmeaUserGroup;

-- COMMAND ----------

--Grant READ Permissions on specific entity/object/table to AMEA Group

GRANT SELECT ON TABLE $CatalogName.$SchemaName.vw_FACT_LINEITEM TO $AmeaUserGroup;
GRANT SELECT ON TABLE $CatalogName.$SchemaName.vw_DIM_CUSTOMER TO $AmeaUserGroup;
GRANT SELECT ON TABLE $CatalogName.$SchemaName.vw_DIM_ORDER TO $AmeaUserGroup;
GRANT SELECT ON TABLE $CatalogName.$SchemaName.DIM_PART TO $AmeaUserGroup;
GRANT SELECT ON TABLE $CatalogName.$SchemaName.DIM_SUPPLIER TO $AmeaUserGroup;


-- COMMAND ----------

GRANT SELECT ON TABLE $CatalogName.$SchemaName.vw_FACT_LINEITEM TO $EmeaUserGroup;
GRANT SELECT ON TABLE $CatalogName.$SchemaName.vw_DIM_CUSTOMER TO $EmeaUserGroup;
GRANT SELECT ON TABLE $CatalogName.$SchemaName.vw_DIM_ORDER TO $EmeaUserGroup;
GRANT SELECT ON TABLE $CatalogName.$SchemaName.DIM_PART TO $EmeaUserGroup;
GRANT SELECT ON TABLE $CatalogName.$SchemaName.DIM_SUPPLIER TO $EmeaUserGroup;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Show Granting**: Lists all privileges that are granted on a securable.

-- COMMAND ----------

SHOW GRANTS ON SCHEMA $CatalogName.$SchemaName;

-- COMMAND ----------

SHOW GRANTS ON TABLE $CatalogName.$SchemaName.DIM_SUPPLIER;
