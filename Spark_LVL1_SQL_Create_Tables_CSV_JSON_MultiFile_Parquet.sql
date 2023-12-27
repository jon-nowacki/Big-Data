-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### SQL Data Ingestion
-- MAGIC * This is a SQL Notebook
-- MAGIC * No Python is used
-- MAGIC
-- MAGIC Ingesting data from
-- MAGIC * CSV
-- MAGIC * JSON
-- MAGIC * Complex Structure (name schema) JSON
-- MAGIC * Multi Line JSON
-- MAGIC * Multi-File
-- MAGIC * Parquet

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create tables for CSV files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create circuits table
-- MAGIC * if exists -> rerunnable
-- MAGIC * drop table  (reset)
-- MAGIC * create table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING csv
OPTIONS (path "/mnt/formula1dl/raw/circuits.csv", header true)

-- COMMAND ----------

SELECT circuitId,circuitRef,name,location, country,lat,lng  FROM f1_raw.circuits LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING)
USING csv
OPTIONS (path "/mnt/formula1dl/raw/races.csv", header true)

-- COMMAND ----------

SELECT raceId,year,round,circuitId,name,`date`,`time` FROM f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create tables for JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create constructors table
-- MAGIC * Single Line JSON
-- MAGIC * Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING)
USING json
OPTIONS(path "/mnt/formula1dl/raw/constructors.json")

-- COMMAND ----------

SELECT * FROM f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create drivers table
-- MAGIC * Single Line JSON
-- MAGIC * Complex structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING)
USING json
OPTIONS (path "/mnt/formula1dl/raw/drivers.json")

-- COMMAND ----------

-- MAGIC %md ##### Create results table
-- MAGIC * Single Line JSON
-- MAGIC * Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING)
USING json
OPTIONS(path "/mnt/formula1dl/raw/results.json")

-- COMMAND ----------

SELECT resultId,raceId,driverId,constructorId,number,grid,position,positionText FROM f1_raw.results LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create pit stops table
-- MAGIC * Multi Line JSON
-- MAGIC * Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING)
USING json
OPTIONS(path "/mnt/formula1dl/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

SELECT driverId,duration,lap,milliseconds,raceId,stop,time FROM f1_raw.pit_stops LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create tables for list of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Lap Times Table
-- MAGIC * CSV file
-- MAGIC * Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS (path "/mnt/formula1dl/raw/lap_times")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Lots of files in the directory!

-- COMMAND ----------

-- MAGIC %fs ls /mnt/formula1dl/raw/lap_times/

-- COMMAND ----------

SELECT * FROM f1_raw.lap_times LIMIT 5

-- COMMAND ----------

SELECT COUNT(*) AS entry_count FROM f1_raw.lap_times;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Qualifying Table
-- MAGIC * JSON file
-- MAGIC * MultiLine JSON `OPTIONS (path "xxxx", multiLine true)`
-- MAGIC * Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT)
USING json
  OPTIONS (path "/mnt/formula1dl/raw/qualifying", multiLine true)

-- COMMAND ----------

SELECT constructorId,driverId,number,position,q1,q2,q3,qualifyId FROM f1_raw.qualifying

-- COMMAND ----------

DESC EXTENDED f1_raw.qualifying;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Parquet Files
-- MAGIC

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1dl/processed"

-- COMMAND ----------

-- MAGIC %md

-- COMMAND ----------

DESC DATABASE f1_processed;

-- COMMAND ----------


