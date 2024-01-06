-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### SQL Objects - Managing Data, Metadata and More
-- MAGIC [Spark SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html)
-- MAGIC * Create databases
-- MAGIC * Select Databases
-- MAGIC * Managed Tables (Spark manages metadata AND data)
-- MAGIC * External Tables (Spark only maintains metadata, we maintain data itself)
-- MAGIC * [SQL](https://spark.apache.org/docs/latest/sql-ref.html)
-- MAGIC * [DDL](https://spark.apache.org/docs/latest/sql-ref-syntax.html#ddl-statements)
-- MAGIC * [Create Database](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-database.html)
-- MAGIC * Temp view: one and done projects
-- MAGIC * Global temp view: used by multiple notebooks to create output and then delete the tables
-- MAGIC * Permanent view: Dashboards

-- COMMAND ----------

CREATE DATABASE IF NOT EXIST*S demo;

-- COMMAND ----------

DESCRIBE DATABASE demo;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

show tables;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

use demo  ;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

show tables;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Managed Tables
-- MAGIC [Spark SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html)
-- MAGIC * [SQL](https://spark.apache.org/docs/latest/sql-ref.html)
-- MAGIC * [DDL](https://spark.apache.org/docs/latest/sql-ref-syntax.html#ddl-statements)
-- MAGIC * [Create Database](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-database.html)

-- COMMAND ----------

-- MAGIC %run "./includes/config_file_paths"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

show tables in default;

-- COMMAND ----------

use demo;
show tables;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Location of the File
-- MAGIC LOCATION:
-- MAGIC ```
-- MAGIC dbfs:/user/hive/warehouse/demo.db/race_results_python
-- MAGIC ```

-- COMMAND ----------

DESC EXTENDED race_results_python;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Good Practice: Always specify database
-- MAGIC * Know which database data is coming from 
-- MAGIC * Allows joining tables from different databases
-- MAGIC * Multiple tables from multiple databases need explicit anyway

-- COMMAND ----------

SELECT * 
from demo.race_results_python
where race_year = 2009;

-- COMMAND ----------

create table demo.race_results_sql
AS
select * 
from demo.race_results_python
where race_year = 2020;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### confirm location
-- MAGIC * databse: demo.db
-- MAGIC * table: race_results_sql
-- MAGIC * path `dbfs:/user/hive/warehouse/demo.db/race_results_sql`

-- COMMAND ----------

desc extended demo.race_results_sql;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Managed tables
-- MAGIC * Get deleted from the file system when you drop them.
-- MAGIC * This data can NOT be recovered.

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/demo.db/

-- COMMAND ----------

drop table demo.race_results_sql;

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/demo.db/

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### External Tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet")\
-- MAGIC     .option("path", f"{presentation_folder_path}/race_results_ext_py")\
-- MAGIC         .mode("overwrite") \
-- MAGIC             .saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### External table
-- MAGIC * type: EXTERNAL
-- MAGIC * `dbfs:/mnt/formula1dl/presentation/race_results_ext_py`
-- MAGIC

-- COMMAND ----------

DESCRIBE EXTENDED demo.race_results_ext_py

-- COMMAND ----------

-- MAGIC %fs ls /mnt/formula1dl/presentation/

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql(
  race_year INT,
  race_name STRING,
  race_date TIMESTAMP,
  circuit_location STRING,
  driver_name STRING,
  driver_number INT,
  driver_nationality STRING,
  team STRING,
  grid INT,
  fastest_lap INT,
  race_time STRING,
  points FLOAT,
  position INT,
  created_date TIMESTAMP
)
USING parquet
LOCATION "/mnt/formula1dl/presentation/race_results_ext_sql"

-- COMMAND ----------

SHOW TABLES in demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Notice it's not on the hard drive.
-- MAGIC It exists in Hive metasetore but not in Blob or S3 yet.
-- MAGIC
-- MAGIC Must insert data first:
-- MAGIC
-- MAGIC * https://spark.apache.org/docs/latest/sql-ref-syntax-dml-insert-table.html

-- COMMAND ----------

-- MAGIC %fs ls /mnt/formula1dl/presentation/

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
  SELECT * FROM demo.race_results_ext_py WHERE race_year = 2020;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Files finally Exist on hard drive
-- MAGIC * Blob/S3 storage written to

-- COMMAND ----------

-- MAGIC %fs ls /mnt/formula1dl/presentation/

-- COMMAND ----------

SELECT COUNT(1) from demo.race_results_ext_sql;

-- COMMAND ----------

SHOW TABLES in demo;

-- COMMAND ----------

DROP TABLE race_results_ext_sql;

-- COMMAND ----------

SHOW TABLES in demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Files still exist

-- COMMAND ----------

-- MAGIC %fs ls /mnt/formula1dl/presentation/

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql(
  race_year INT,
  race_name STRING,
  race_date TIMESTAMP,
  circuit_location STRING,
  driver_name STRING,
  driver_number INT,
  driver_nationality STRING,
  team STRING,
  grid INT,
  fastest_lap INT,
  race_time STRING,
  points FLOAT,
  position INT,
  created_date TIMESTAMP
)
USING parquet
LOCATION "/mnt/formula1dl/presentation/race_results_ext_sql"

-- COMMAND ----------

SELECT COUNT(1) from demo.race_results_ext_sql;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Table Views
-- MAGIC * Temp View (only 1 notebook can access)
-- MAGIC * Global View (any notebook on the cluster can access)
-- MAGIC * Permanent View
-- MAGIC
-- MAGIC In the context of databases, both tables and views are database objects used to store and represent data, but they serve different purposes and have distinct characteristics:
-- MAGIC
-- MAGIC Tables:
-- MAGIC * Structure for Storing Data: Tables are fundamental database objects that store data in rows and columns.
-- MAGIC * Physical Storage: They occupy space in the database and contain the actual data. When you insert, update, or delete records, it directly affects the data stored in tables.
-- MAGIC * Schema: Tables have a predefined schema that defines the structure, data types, and constraints for each column.
-- MAGIC * Persistency: Tables persist data, meaning the data remains in the table until explicitly removed.
-- MAGIC * Primary Source of Data: Tables are the primary storage units for data in a database system.
-- MAGIC Views:
-- MAGIC * Virtual Representation: Views are virtual tables generated by a query that retrieves data from one or more tables.
-- MAGIC * Stored Query Results: Unlike tables, views do not contain the actual data; they represent the result set of a stored query.
-- MAGIC * Dynamic Data Presentation: They present data in a specific way by defining a subset of data, joining multiple tables, or applying certain filters or transformations.
-- MAGIC * Schema: Views have their own schema based on the query used to create them.
-- MAGIC * Abstraction Layer: Views serve as an abstraction layer over underlying tables, allowing users to access data without needing to know the complexity of the underlying table structure or relationships.
-- MAGIC * Security and Access Control: Views can be used to restrict access to certain columns or rows of data, providing a security layer.
-- MAGIC Key Differences:
-- MAGIC * Data Storage: Tables store actual data, while views store queries/results.
-- MAGIC * Modification: Tables can have data inserted, updated, or deleted directly, while views are read-only by default (some views can be updatable depending on certain conditions).
-- MAGIC * Persistence: Tables persistently store data, while views are dynamic and show the most current data from underlying tables.
-- MAGIC * Schema: Tables have a fixed schema, while views' schema depends on the query used to create them.
-- MAGIC
-- MAGIC In summary, tables are physical structures that store data, whereas views are virtual representations of data based on queries and provide a way to present data in a specific format without storing it physically. Views often offer a way to simplify complex queries or restrict access to certain parts of the data for security or abstraction purposes.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS
SELECT * 
FROM demo.race_results_python
WHERE race_year = 2018;

-- COMMAND ----------

SELECT * FROM v_race_results

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_grace_results
AS
SELECT * 
FROM demo.race_results_python
WHERE race_year = 2012;

-- COMMAND ----------

SELECT * 
FROM global_temp.gv_grace_results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Permanent View
-- MAGIC * visible even after detach & reattach to cluster

-- COMMAND ----------

CREATE OR REPLACE  VIEW demo.pv_grace_results
AS
SELECT * 
FROM demo.race_results_python
WHERE race_year = 2000;

-- COMMAND ----------

SHOW Tables in demo;

-- COMMAND ----------

-- MAGIC   %md 
-- MAGIC   #

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------


