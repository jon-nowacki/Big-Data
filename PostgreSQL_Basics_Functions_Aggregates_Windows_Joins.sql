-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

USE f1_processed;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT *
FROM f1_processed.drivers
WHERE nationality = "British"
and dob >= '1990-01-01'
LIMIT 10;

-- COMMAND ----------

DESC drivers;

-- COMMAND ----------

SELECT * 
FROM drivers
WHERE nationality = 'British'
AND dob >= '1990-01-01'

-- COMMAND ----------

SELECT name, dob AS date_of_birth
FROM drivers
WHERE nationality = 'British'
AND dob >= '1990-01-01'

-- COMMAND ----------

SELECT name, dob AS date_of_birth
FROM drivers
WHERE nationality = 'British'
AND dob >= '1990-01-01'
ORDER BY dob

-- COMMAND ----------

SELECT name, dob AS date_of_birth
FROM drivers
WHERE nationality = 'British'
AND dob >= '1990-01-01'
ORDER BY dob DESC;

-- COMMAND ----------

SELECT *
FROM drivers
ORDER BY nationality ASC,
dob DESC;

-- COMMAND ----------

SELECT name, dob AS date_of_birth
FROM drivers
WHERE (nationality = 'British'
AND dob >= '1990-01-01')
OR nationality = 'Indian'
ORDER BY dob DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Functions
-- MAGIC
-- MAGIC * [Latest API docs](https://spark.apache.org/docs/latest/)
-- MAGIC * [SQL Functions](https://spark.apache.org/docs/latest/api/sql/index.html)
-- MAGIC

-- COMMAND ----------

use f1_processed;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT driver_id,driver_ref,number,code,name,dob,nationality
FROM drivers

-- COMMAND ----------

SELECT driver_id,driver_ref,number,code,name,dob,nationality, 
CONCAT(driver_ref, '-', code) AS new_driver_ref
FROM drivers

-- COMMAND ----------

SELECT driver_id,driver_ref,number,code,name,dob,nationality, 
SPLIT(name, ' ')
FROM drivers

-- COMMAND ----------

SELECT driver_id,driver_ref,number,code,name,dob,nationality, 
SPLIT(name, ' ') [0] forename, SPLIT(name, ' ') [1] surname
FROM drivers

-- COMMAND ----------

SELECT driver_id,driver_ref,number,current_timestamp()
FROM drivers

-- COMMAND ----------

SELECT driver_id,driver_ref,number,date_format(dob, 'dd-MM-yyyy')
FROM drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Date Add
-- MAGIC * [Date Add](https://spark.apache.org/docs/latest/api/sql/index.html#date_add)

-- COMMAND ----------

SELECT driver_id,driver_ref,number, date_add(dob, 11111)
FROM drivers

-- COMMAND ----------

SELECT COUNT(*)
FROM drivers
WHERE nationality='British'

-- COMMAND ----------

SELECT nationality, COUNT(*)
FROM drivers
GROUP BY nationality
ORDER BY nationality

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Having
-- MAGIC More than  >100 drivers per group

-- COMMAND ----------

SELECT nationality, COUNT(*)
FROM drivers
GROUP BY nationality
HAVING COUNT(*) > 100
ORDER BY nationality

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Rank by country
-- MAGIC
-- MAGIC Ranking by age_rank may, in certain situations, be redundant.

-- COMMAND ----------

SELECT nationality, name, dob, rank() OVER (PARTITION BY nationality ORDER BY dob DESC) as age_rank
FROM drivers
ORDER BY nationality, age_rank

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Joins
-- MAGIC
-- MAGIC
-- MAGIC * [Latest API docs](https://spark.apache.org/docs/latest/sql-programming-guide.html)
-- MAGIC * [SQL Data Retrieval](https://spark.apache.org/docs/latest/sql-ref-syntax.html#data-retrieval-statements)
-- MAGIC * [SQL JOIN](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-join.html)
-- MAGIC * [SQL JOIN Types](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-join.html#join-types)
-- MAGIC

-- COMMAND ----------

use f1_presentation;

-- COMMAND ----------

show tables;

-- COMMAND ----------

CREATE or REPLACE TEMP VIEW v_driver_standings_2018
AS
SELECT race_year, driver_name, team, total_points, wins, rank
from driver_standings
where race_year = 2018;

-- COMMAND ----------

SELECT * from v_driver_standings_2018

-- COMMAND ----------

CREATE or REPLACE TEMP VIEW v_driver_standings_2020
AS
SELECT race_year, driver_name, team, total_points, wins, rank
from driver_standings
where race_year = 2020;

-- COMMAND ----------

SELECT * from v_driver_standings_2020

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Only returns drivers that raced in both years.

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
JOIN v_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### Left Join
-- MAGIC * Shows all drivers that raced in 2018
-- MAGIC * Some nulls for 2020

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
LEFT JOIN v_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### Right Join
-- MAGIC * Shows all drivers that raced in 2020
-- MAGIC * Some nulls for 2018
-- MAGIC * RIGHT JOIN vs RIGHT OUTER JOIN is the same things

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
RIGHT JOIN v_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
RIGHT OUTER JOIN v_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### Full outer JOIN (superset)
-- MAGIC * Shows all drivers that raced in 2020 or 2018
-- MAGIC * Some nulls for 2018
-- MAGIC * FULL JOIN vs FULL OUTER JOIN is the same things

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
FULL OUTER JOIN v_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### Semi Join
-- MAGIC * Inner join but you only get left side of join
-- MAGIC * only get 2018
-- MAGIC * 15 records, same as inner join

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
SEMI JOIN v_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Anti-Join
-- MAGIC * drivers that raced in 2018 but NOT in 2020

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
ANTI JOIN v_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### cross Join
-- MAGIC * Cartesian product

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
CROSS JOIN v_driver_standings_2020 d_2020

-- COMMAND ----------


