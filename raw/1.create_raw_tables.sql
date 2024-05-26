-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

--create external table for circuits
DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
  circuitId INT,
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

--create external table for races
DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING csv
OPTIONS (path "/mnt/formula1dl/raw/races.csv", header true)

-- COMMAND ----------

--create external table for constructors
DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
USING json 
OPTIONS (path "/mnt/formula1dl/raw/constructors.json")

-- COMMAND ----------

--create external table for drivers
DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING json
OPTIONS (path "/mnt/formula1dl/raw/drivers.json")

-- COMMAND ----------

--create external table for results
DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
  resultId INT,
  raceId INT, 
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
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
  statusId STRING
)
USING json 
OPTIONS (path "/mnt/formula1dl/raw/results.json")

-- COMMAND ----------

--create external table for pitstops
DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
  raceId INT,
  driverId INT,
  stop INT,
  lap INT,
  time STRING,
  duration STRING,
  milliseconds INT
)
USING json 
OPTIONS (path "/mnt/formula1dl/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

--create external table for laptimes
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

--create external table for qualifying
DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
  qualifyingId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING
)
USING json
OPTIONS (path "/mnt/formula1dl/raw/qualifying", multiLine true)