-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Create and Grant Access to Data Objects

-- COMMAND ----------

-- MAGIC %md
-- MAGIC As a metastore admin, you can grant yourself access to any object within the metastore. Begin by using the catalog `main`, which refers to a new metastore as created within Unity Catalog, as opposed to the default hive metastore. This will make it so we can use and create tables within the `main.default` schema.

-- COMMAND ----------

USE CATALOG main;
GRANT USAGE, CREATE ON CATALOG main TO `gurpreet.sethi+uc@databricks.com`;

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS acme_emea;
GRANT USAGE, CREATE ON CATALOG acme_emea TO `gurpreet.sethi+uc@databricks.com`;
GRANT USAGE, CREATE ON CATALOG acme_emea TO `EMEA`;

-- COMMAND ----------

USE CATALOG acme_emea;
CREATE SCHEMA IF NOT EXISTS hr_data;
GRANT USAGE, CREATE on SCHEMA acme_emea.hr_data to `EMEA`;
GRANT USAGE, CREATE on SCHEMA acme_emea.hr_data to `gurpreet.sethi+uc@databricks.com`

-- COMMAND ----------

SHOW GRANT on SCHEMA acme_emea.hr_data;

-- COMMAND ----------

SHOW CATALOGS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Next, you'll create a table in Unity Catalog's default schema, or database, and then insert some data using the SQL statements below.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS hr_data.team_members
(
   empId   INT,
   empEmail  STRING,
   location  STRING
);

INSERT INTO hr_data.team_members VALUES
  (10, 'jim@unitycatalog.com', 'EDINBURGH'),
  (20, 'pam@unitycatalog.com', 'PADDINGTON'),
  (30, 'dwight@unitycatalog.com', 'MAIDSTONE'),
  (40, 'michael@unitycatalog.com', 'DARLINGTON'),
  (50, 'stanley@unitycatalog.com', 'BIRMINGHAM');

-- COMMAND ----------

SELECT * FROM hr_data.team_members LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now that you've created the `team_members` table in the `default` schema located in the `main` metastore, provision access to your table by using a group principal. If you'd like to make this table accessible to all users, you can use the default group `account users`.

-- COMMAND ----------

GRANT SELECT ON hr_data.team_members TO `account users`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You can double check your work by using the `show grant` command for your `account users` group. This will output the kinds of access a given principal has for a given data object, such as a catalog, schema, or table.

-- COMMAND ----------

SHOW GRANT `account users` ON TABLE main.default.team_members

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now you can navigate to Databricks SQL to perform the same operation, though this time you'll do it through the user interface.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Explore Unity Catalog's Three Level Namespace
-- MAGIC Unity Catalog introduces a 3-level-namespace so that each table is identified by a `catalog.schema.table` definition. This has the added benefit of allowing users to access Unity Catalog tables at the same time as those tables from a legacy databricks hosted metastore, or those from other catalogs created in this way.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In this example, you'll create two tables in two separate catalogs and then join them in Databricks SQL, using Unity Catalog's three layer name-spacing to perform this cross-catalog join. Start by creating the two tables you'll need in order to do the join later.

-- COMMAND ----------

-- create the dim_department table in the databricks hosted hive metastore
USE CATALOG hive_metastore;
CREATE TABLE IF NOT EXISTS default.dim_department
(
   DEPTCODE   INT,
   DeptName   STRING,
   LOCATION   STRING
);

INSERT INTO default.dim_department VALUES (10, 'FINANCE', 'EDINBURGH'),
                                          (20,'SOFTWARE','PADDINGTON'),
                                          (30, 'SALES', 'MAIDSTONE'),
                                          (40,'MARKETING', 'DARLINGTON'),
                                          (50,'ADMIN', 'BIRMINGHAM');

-- COMMAND ----------

-- create the dim_employee table in a unity catalog metastore
USE CATALOG acme_emea;
CREATE TABLE IF NOT EXISTS hr_data.dim_employee
(
   EmpCode      INT,
   EmpFName     STRING,
   EmpLName     STRING,
   Job          STRING,
   Manager      STRING,
   HireDate     DATE,
   Salary       INT,
   Commission   INT,
   DEPTCODE     INT
);
                       
INSERT INTO hr_data.dim_employee  
VALUES (9369, 'TONY', 'STARK', 'SOFTWARE ENGINEER', 7902, '1980-12-17', 2800,0,20),
       (9499, 'TIM', 'ADOLF', 'SALESMAN', 7698, '1981-02-20', 1600, 300,30),    
       (9566, 'KIM', 'JARVIS', 'MANAGER', 7839, '1981-04-02', 3570,0,20),
       (9654, 'SAM', 'MILES', 'SALESMAN', 7698, '1981-09-28', 1250, 1400, 30),
       (9782, 'KEVIN', 'HILL', 'MANAGER', 7839, '1981-06-09', 2940,0,10),
       (9788, 'CONNIE', 'SMITH', 'ANALYST', 7566, '1982-12-09', 3000,0,20),
       (9839, 'ALFRED', 'KINSLEY', 'PRESIDENT', 7566, '1981-11-17', 5000,0, 10),
       (9844, 'PAUL', 'TIMOTHY', 'SALESMAN', 7698, '1981-09-08', 1500,0,30),
       (9876, 'JOHN', 'ASGHAR', 'SOFTWARE ENGINEER', 7788, '1983-01-12',3100,0,20),
       (9900, 'ROSE', 'SUMMERS', 'TECHNICAL LEAD', 7698, '1981-12-03', 2950,0, 20),
       (9902, 'ANDREW', 'FAULKNER', 'ANAYLYST', 7566, '1981-12-03', 3000,0, 10),
       (9934, 'KAREN', 'MATTHEWS', 'SOFTWARE ENGINEER', 7782, '1982-01-23', 3300,0,20),
       (9591, 'WENDY', 'SHAWN', 'SALESMAN', 7698, '1981-02-22', 500,0,30),
       (9698, 'BELLA', 'SWAN', 'MANAGER', 7839, '1981-05-01', 3420, 0,30),
       (9777, 'MADII', 'HIMBURY', 'ANALYST', 7839, '1981-05-01', 2000, 200, NULL),
       (9860, 'ATHENA', 'WILSON', 'ANALYST', 7839, '1992-06-21', 7000, 100, 50),
       (9861, 'JENNIFER', 'HUETTE', 'ANALYST', 7839, '1996-07-01', 5000, 100, 50);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Migrate a Table to Unity Catalog
-- MAGIC Unity Catalog makes moving data across catalogs simple. In a previous example, you created tables in two separate metastores and performed a join across them. Now you will create a table in a Unity Catalog created metastore by using a simple `CREATE TABLE AS` statement to source data from another metastore.

-- COMMAND ----------

USE CATALOG main;
--- creating a table in the unity catalog metastore from the same table in the legacy hosted metastore
CREATE TABLE IF NOT EXISTS default.dim_department AS SELECT * FROM hive_metastore.default.dim_department;

-- COMMAND ----------

-- Grants access to all principals in databricks
GRANT SELECT on main.default.dim_department to `account users`;

-- COMMAND ----------

--- let's check the results
select * from main.default.dim_department;
