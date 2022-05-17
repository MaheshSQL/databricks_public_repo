-- Databricks notebook source
-- MAGIC %md #Load TPC-H Data

-- COMMAND ----------

CREATE WIDGET text CatalogName DEFAULT "`uc-demo-tpch`";
CREATE WIDGET text SchemaName DEFAULT "tpch";


-- COMMAND ----------

USE CATALOG $CatalogName;

-- COMMAND ----------

USE $SchemaName;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS $SchemaName COMMENT 'The TPC Benchmark™H (TPC-H) is a decision support benchmark. It consists of a suite of business oriented ad-hoc
queries and concurrent data modifications. The queries and the data populating the database have been chosen to have broad industry-wide relevance while maintaining a sufficient degree of ease of implementation.' WITH DBPROPERTIES ( "sf" = 1 );

USE DATABASE $SchemaName;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS CUSTOMER (
  C_CUSTKEY INTEGER NOT NULL COMMENT 'Customer Primary Key',
  C_NAME VARCHAR(25) NOT NULL COMMENT 'Customer Name',
  C_ADDRESS VARCHAR(40) NOT NULL COMMENT 'Cusotmer Address',
  C_NATIONKEY INTEGER NOT NULL COMMENT 'Customer Nation foreign key',
  C_PHONE VARCHAR(15) NOT NULL COMMENT 'Customer Phone Number',
  C_ACCTBAL DECIMAL(15, 2) NOT NULL COMMENT 'Customers Account Balance',
  C_MKTSEGMENT VARCHAR(10) NOT NULL COMMENT 'Customer Market Segment',
  C_COMMENT VARCHAR(117) NOT NULL COMMENT 'Comments about the customer'
) USING DELTA COMMENT 'Customer Table';
CREATE TABLE IF NOT EXISTS LINEITEM (
  L_ORDERKEY integer COMMENT 'Order Primary Key',
  L_PARTKEY integer COMMENT 'Part foreign key',
  L_SUPPKEY integer COMMENT 'Supplier foreign key',
  L_LINENUMBER integer COMMENT 'Line item line number',
  L_QUANTITY numeric (20, 2) COMMENT 'Number of items on line',
  L_EXTENDEDPRICE numeric (20, 2) COMMENT 'Extended Price',
  L_DISCOUNT numeric (3, 2) COMMENT 'Discount on line item',
  L_TAX numeric (3, 2) COMMENT 'Calculated tax',
  L_RETURNFLAG varchar(1) COMMENT 'Line item return flag',
  L_LINESTATUS varchar(1) COMMENT 'Line item line status ',
  L_SHIPDATE date COMMENT '',
  L_COMMITDATE date COMMENT '',
  L_RECEIPTDATE date COMMENT '',
  L_SHIPINSTRUCT varchar(25) COMMENT '',
  L_SHIPMODE varchar(10) COMMENT '',
  L_COMMENT varchar(44) COMMENT ''
) USING DELTA PARTITIONED BY (L_RECEIPTDATE) COMMENT 'Line items from the orders table';
CREATE TABLE IF NOT EXISTS NATION (
  N_NATIONKEY integer COMMENT 'Nation primary key',
  N_NAME varchar(25) COMMENT 'Nation Name',
  N_REGIONKEY integer COMMENT 'Region foreign key',
  N_COMMENT varchar(152) COMMENT 'Nation comments'
) USING DELTA COMMENT 'Nation table';
CREATE TABLE IF NOT EXISTS ORDERS (
  O_ORDERKEY integer COMMENT 'Orders primary key',
  O_CUSTKEY integer COMMENT 'Customer foreign key',
  O_ORDERSTATUS varchar(1) COMMENT 'Order Status',
  O_TOTALPRICE numeric (20, 2) COMMENT 'Total price for the order',
  O_ORDERDATE date COMMENT 'Order Date',
  O_ORDERPRIORITY varchar(15) COMMENT 'Order priority ',
  O_CLERK varchar(15) COMMENT 'Order Clerk',
  O_SHIPPRIORITY integer COMMENT 'Order shipment priority',
  O_COMMENT varchar(79) COMMENT 'Order comments'
) USING DELTA PARTITIONED BY (O_ORDERDATE)  COMMENT 'Orders table';
CREATE TABLE IF NOT EXISTS PART (
  P_PARTKEY integer COMMENT 'Part primary key',
  P_NAME varchar(55) COMMENT 'Part name',
  P_MFGR varchar(25) COMMENT 'Part Manufacturer',
  P_BRAND varchar(10) COMMENT 'Part brand',
  P_TYPE varchar(25) COMMENT 'Part type',
  P_SIZE integer COMMENT 'Part size',
  P_CONTAINER varchar(10) COMMENT 'Part container',
  P_RETAILPRICE numeric (20, 2) COMMENT 'Part retail price',
  P_COMMENT varchar(23) COMMENT 'Part comment'
) USING DELTA  COMMENT 'Part table';
CREATE TABLE IF NOT EXISTS PARTSUPP (
  PS_PARTKEY integer COMMENT 'Part supplier primary key',
  PS_SUPPKEY integer COMMENT 'Supplier foreign key',
  PS_AVAILQTY integer COMMENT 'Available quantity of the part for the supplier',
  PS_SUPPLYCOST numeric (20, 2) COMMENT 'The supplier cost',
  PS_COMMENT varchar(199) COMMENT 'Comments for the part supplier'
) USING DELTA COMMENT 'Part Supplier cross reference table';
CREATE TABLE IF NOT EXISTS REGION (
  R_REGIONKEY integer COMMENT 'Region primary key',
  R_NAME varchar(25) COMMENT 'Region name',
  R_COMMENT varchar(152) COMMENT 'Region comments'
) USING DELTA COMMENT '';
CREATE TABLE IF NOT EXISTS SUPPLIER (
  S_SUPPKEY integer COMMENT 'Supplier primary key',
  S_NAME varchar(25) COMMENT 'Supplier name',
  S_ADDRESS varchar(40) COMMENT 'Supplier address',
  S_NATIONKEY integer COMMENT 'Nation foreign key',
  S_PHONE varchar(15) COMMENT 'Supplier phone number',
  S_ACCTBAL numeric (20, 2) COMMENT 'Supplier account balance',
  S_COMMENT varchar(101) COMMENT 'Supplier comments'
) USING DELTA COMMENT '';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tablesToProcess = ['customer','lineitem','part','partsupp','nation','orders','region','supplier']

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC def insertData(tableName):
-- MAGIC   file_location = f"dbfs:/databricks-datasets/tpch/data-001/{tableName}/{tableName}.tbl"
-- MAGIC   tableDF = (spark.read.format("csv")
-- MAGIC              .option("inferSchema", "true")
-- MAGIC              .option("header", "false")
-- MAGIC              .option("sep", "|")
-- MAGIC              .option("mode", "DROPMALFORMED")
-- MAGIC              .load(file_location)
-- MAGIC             )
-- MAGIC   tableDF = tableDF.drop(tableDF.columns[-1])
-- MAGIC   tableDF.createOrReplaceTempView("insertData")
-- MAGIC   spark.sql(f"insert into {tableName} select * from insertData")
-- MAGIC   
-- MAGIC def optimizeTable(tableName, columnNames):
-- MAGIC   spark.sql(f"OPTIMIZE {tableName} zorder by {columnNames};")
-- MAGIC   spark.sql(f"ANALYZE TABLE {tableName} COMPUTE STATISTICS FOR ALL COLUMNS")

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC 
-- MAGIC for x in tablesToProcess:
-- MAGIC   print(x)
-- MAGIC   insertData(x)  

-- COMMAND ----------

create table if not exists tableOptimizations
(tableName string, zorderKeys  string)

-- COMMAND ----------

insert into tableOptimizations select "LINEITEM" , "L_PARTKEY" ;
 
insert into tableOptimizations select "ORDERS" , "O_CUSTKEY" ;
 
insert into tableOptimizations select "CUSTOMER" , "C_NATIONKEY" ;
 
insert into tableOptimizations select "PARTSUPP" , "PS_SUPPKEY" ;
 
insert into tableOptimizations select "PARTSUPP" , "PS_SUPPKEY, PS_PARTKEY" ;

insert into tableOptimizations select "SUPPLIER" , "S_NATIONKEY" ;
 
insert into tableOptimizations select "region" , "R_REGIONKEY" ;

-- COMMAND ----------

select * from tableOptimizations

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC df = spark.sql("select * from tableOptimizations")
-- MAGIC 
-- MAGIC for x in df.rdd.collect():
-- MAGIC   optimizeTable(x["tableName"], x["zorderKeys"])
-- MAGIC   

-- COMMAND ----------

ALTER TABLE REGION
ADD COLUMN TEAM_OWNER STRING COMMENT 'Team that owns this region'

-- COMMAND ----------

USE CATALOG $CatalogName;
USE DATABASE $SchemaName;

UPDATE REGION
SET TEAM_OWNER = 'gsethi-uc-demo-emea'
--WHERE TEAM_OWNER='EMEA';
WHERE R_NAME IN ('AFRICA','EUROPE','MIDDLE EAST');

UPDATE REGION
SET TEAM_OWNER = 'gsethi-uc-demo-amea'
--WHERE TEAM_OWNER='AMER';
WHERE R_NAME IN ('AMERICA');

UPDATE REGION
SET TEAM_OWNER = 'gsethi-uc-demo-apac'
--WHERE TEAM_OWNER='APAC';
WHERE R_NAME IN ('ASIA');

-- COMMAND ----------

SELECT DISTINCT TEAM_OWNER FROM REGION

-- COMMAND ----------

CREATE TABLE F_LINEITEM AS 
SELECT
  L_ORDERKEY as ORDERKEY,
  o.O_CUSTKEY as CUSTKEY,
  L_PARTKEY as PARTKEY,
  L_SUPPKEY as SUPPKEY,
  r.TEAM_OWNER AS TEAM_OWNER,
  o.O_ORDERDATE AS ORDER_DATE,
  L_SHIPDATE as SHIP_DATE,
  L_COMMITDATE as COMMIT_DATE,
  L_RECEIPTDATE as RECEIPT_DATE,
  L_LINENUMBER as LINE_NUMBER,
  L_QUANTITY as QUANTITY,
  L_EXTENDEDPRICE as EXTENDED_PRICE,
  L_DISCOUNT as DISCOUNT,
  L_TAX as TAX
FROM LINEITEM li
JOIN ORDERS o ON li.L_ORDERKEY = o.O_ORDERKEY
JOIN CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY
JOIN NATION n on C.C_NATIONKEY = n.N_NATIONKEY
JOIN REGION r on n.N_REGIONKEY = r.R_REGIONKEY

-- COMMAND ----------

CREATE TABLE D_ORDER AS 
SELECT 
  o.O_ORDERKEY as ORDERKEY,
  r.TEAM_OWNER AS TEAM_OWNER,
  o.O_ORDERSTATUS as ORDER_STATUS,
  o.O_ORDERPRIORITY as ORDER_PRIORITY,
  o.O_CLERK as ORDER_CLERK,
  o.O_SHIPPRIORITY as SHIP_PRIORITY
FROM ORDERS o
JOIN CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY
JOIN NATION n on C.C_NATIONKEY = n.N_NATIONKEY
JOIN REGION r on n.N_REGIONKEY = r.R_REGIONKEY

-- COMMAND ----------

CREATE TABLE D_CUSTOMER AS
SELECT
  c.C_CUSTKEY as CUSTKEY,
  r.TEAM_OWNER AS TEAM_OWNER,
  c.C_NAME as CUST_NAME,
  c.C_PHONE as CUST_PHONE,
  c.C_MKTSEGMENT as CUST_MKTSEGMENT,
  c.C_COMMENT as CUST_COMMENT,
  c.C_ADDRESS as CUST_ADDRESS,
  n.N_NAME as NATION_NAME,
  n.N_COMMENT as NATION_COMMENT,
  r.R_NAME as REGION_NAME,
  r.R_COMMENT as REGION_COMMENT
FROM CUSTOMER c 
JOIN NATION n on C.C_NATIONKEY = n.N_NATIONKEY
JOIN REGION r on n.N_REGIONKEY = r.R_REGIONKEY

-- COMMAND ----------

CREATE TABLE D_PART AS
SELECT
  P_PARTKEY as PARTKEY,
  P_NAME as PART_NAME,
  P_MFGR as PART_MFGR,
  P_BRAND as PART_BRAND,
  P_TYPE as PART_TYPE,
  P_SIZE as PART_SIZE,
  P_CONTAINER as PART_CONTAINER,
  P_RETAILPRICE as PART_RETAILPRICE,
  P_COMMENT as PART_COMMENT
FROM PART  

-- COMMAND ----------

CREATE TABLE D_SUPPLIER AS
SELECT
 s.S_SUPPKEY as SUPPKEY,
 s.S_NAME as SUPPLIER_NAME,
 s.S_ADDRESS as SUPPLIER_ADDRESS,
 s.S_PHONE as SUPPLIER_PHONE,
 s.S_COMMENT as SUPPLIER_COMMENT,
 n.N_NAME as NATION_NAME,
 n.N_COMMENT as NATION_COMMENT,
 r.R_NAME as REGION_NAME,
 r.R_COMMENT as REGION_COMMENT
FROM SUPPLIER s
JOIN NATION n ON s.S_NATIONKEY = n.N_NATIONKEY
JOIN REGION r on n.N_REGIONKEY = r.R_REGIONKEY

-- COMMAND ----------

select is_member('gsethi-uc-demo-emea'), is_member('gsethi-uc-demo-amea')

-- COMMAND ----------

CREATE VIEW F_LINEITEM_RESTRICTED AS
SELECT
*
FROM F_LINEITEM f
WHERE is_member(TEAM_OWNER)

-- COMMAND ----------

CREATE VIEW D_ORDER_RESTRICTED AS
SELECT
*
FROM D_ORDER
WHERE is_member(TEAM_OWNER)

-- COMMAND ----------

CREATE VIEW D_CUSTOMER_RESTRICTED AS
SELECT
*
FROM D_CUSTOMER
WHERE is_member(TEAM_OWNER)
