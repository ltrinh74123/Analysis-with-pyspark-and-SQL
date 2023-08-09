-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## DATA3404 Assignment 1
-- MAGIC
-- MAGIC This is the SQL notebook for the assignment on Databricks, 2023s1.
-- MAGIC
-- MAGIC This notebook assumes that you have executed the **Bootstrap** notebook first.
-- MAGIC
-- MAGIC As the first step, we are mapping the imported CSV files as SQL tables so that they are available for subsequent SQL queries wiht a number of tables: **Aircrafts**, **Airlines**, **Airports** and three variants of the **Flights_**_scale_ table. You can switch between differenty dataset sizes by referring to _either_ the **Flights_small**, _or_ **Flights_medium** _or_ **Flights_large** tables.

-- COMMAND ----------

-- Databricks SQL notebook source
DROP TABLE IF EXISTS Aircrafts;
CREATE TABLE Aircrafts
USING csv
OPTIONS (path "/FileStore/tables/ontimeperformance_aircrafts.csv", header "true");

DROP TABLE IF EXISTS Airlines;
CREATE TABLE Airlines
USING csv
OPTIONS (path "/FileStore/tables/ontimeperformance_airlines.csv", header "true");

DROP TABLE IF EXISTS Airports;
CREATE TABLE Airports
USING csv
OPTIONS (path "/FileStore/tables/ontimeperformance_airports.csv", header "true");

DROP TABLE IF EXISTS Flights_small;
CREATE TABLE Flights_small (
  carrier_code char(2), 
  flight_number int, 
  flight_date date, 
  origin char(3), 
  destination char(3), 
  tail_number varchar(10), 
  scheduled_depature_time char(4), 
  scheduled_arrival_time char(4), 
  actual_departure_time char(4), 
  actual_arrival_time char(4), 
  distance float)
USING csv
OPTIONS (path "/FileStore/tables/ontimeperformance_flights_small.csv", header "true");

DROP TABLE IF EXISTS Flights_medium;
CREATE TABLE Flights_medium (
  carrier_code char(2), 
  flight_number int, 
  flight_date date, 
  origin char(3), 
  destination char(3), 
  tail_number varchar(10), 
  scheduled_depature_time char(4), 
  scheduled_arrival_time char(4), 
  actual_departure_time char(4), 
  actual_arrival_time char(4), 
  distance float)
USING csv
OPTIONS (path "/FileStore/tables/ontimeperformance_flights_medium.csv", header "true");

DROP TABLE IF EXISTS Flights_large;
CREATE TABLE Flights_large (
  carrier_code char(2), 
  flight_number int, 
  flight_date date, 
  origin char(3), 
  destination char(3), 
  tail_number varchar(10), 
  scheduled_depature_time char(4), 
  scheduled_arrival_time char(4), 
  actual_departure_time char(4), 
  actual_arrival_time char(4), 
  distance float)
USING csv
OPTIONS (path "/FileStore/tables/ontimeperformance_flights_large.csv", header "true");


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Individual Questions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Question 1: Determine the name of the 3 airlines with the most aircrafts.
-- MAGIC ####Same code can be used for databricks and Pgadmin

-- COMMAND ----------

--- Small Dataset
SELECT name, count(aircraft_type) as count_of_ac
FROM Aircrafts A
JOIN Flights_small F ON A.tailnum = F.tail_number
JOIN Airlines A2 ON F.carrier_code = A2.carrier_code
GROUP BY name
ORDER BY count_of_ac DESC
LIMIT 3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Team Questions
-- MAGIC ####Note: I did question 2 by myself, thus only kept the code for it. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Question 2
-- MAGIC  
-- MAGIC We call any flight with a duration longer than the average flight time of all flights a long
-- MAGIC flight. Determine the top 5 airports which have the most long flights arriving. For each
-- MAGIC of those airports, determine the airline with the most long flights arriving there.
-- MAGIC

-- COMMAND ----------

--- Small Dataset
WITH new_flight_small AS (SELECT carrier_code, flight_number, flight_date, origin, destination, distance,
    CASE 
        WHEN actual_arrival_time = '2400' THEN '0000'
        ELSE actual_arrival_time
    END as actual_arrival_time,
	CASE 
        WHEN actual_departure_time = '2400' THEN '0000'
        ELSE actual_departure_time
    END as actual_departure_time		
FROM flights_small),

airline_mins AS (
SELECT P.iata, L.name, (UNIX_TIMESTAMP(CONCAT('1970-01-01 ', actual_arrival_time), 'yyyy-MM-dd HHmm') - UNIX_TIMESTAMP(CONCAT('1970-01-01 ',  actual_departure_time), 'yyyy-MM-dd HHmm'))/ 60 AS minutes
FROM new_flight_small S
LEFT JOIN Airports P
ON P.iata = S.destination
LEFT JOIN Airlines L
ON L.carrier_code = S.carrier_code
WHERE (UNIX_TIMESTAMP(CONCAT('1970-01-01 ', actual_arrival_time), 'yyyy-MM-dd HHmm') -
    UNIX_TIMESTAMP(CONCAT('1970-01-01 ',  actual_departure_time), 'yyyy-MM-dd HHmm'))
    / 60 > (SELECT 
    MEAN((UNIX_TIMESTAMP(CONCAT('1970-01-01 ', actual_arrival_time), 'yyyy-MM-dd HHmm') -
    UNIX_TIMESTAMP(CONCAT('1970-01-01 ',  actual_departure_time), 'yyyy-MM-dd HHmm'))
    / 60) FROM new_flight_small)),
    

long_flights_per_airport AS (
SELECT  P.iata, count(*) as long_flights_count_airport
FROM new_flight_small S
JOIN Airports P
ON P.iata = S.destination
LEFT JOIN Airlines L
ON L.carrier_code = S.carrier_code
WHERE P.iata IN (SELECT P.iata
FROM new_flight_small S
LEFT JOIN Airports P
ON P.iata = S.destination
LEFT JOIN Airlines L
ON L.carrier_code = S.carrier_code
WHERE (UNIX_TIMESTAMP(CONCAT('1970-01-01 ', actual_arrival_time), 'yyyy-MM-dd HHmm') -
    UNIX_TIMESTAMP(CONCAT('1970-01-01 ',  actual_departure_time), 'yyyy-MM-dd HHmm'))
    / 60 > (SELECT 
    MEAN((UNIX_TIMESTAMP(CONCAT('1970-01-01 ', actual_arrival_time), 'yyyy-MM-dd HHmm') -
    UNIX_TIMESTAMP(CONCAT('1970-01-01 ',  actual_departure_time), 'yyyy-MM-dd HHmm'))
    / 60)
FROM new_flight_small)
)
GROUP BY P.iata
ORDER BY count(*) DESC
LIMIT 5),

long_flights_airlines AS (
SELECT P.iata, L.name, count(L.name) as name_count
FROM new_flight_small S
JOIN Airports P
ON P.iata = S.destination
LEFT JOIN Airlines L
ON L.carrier_code = S.carrier_code
WHERE P.iata in (SELECT P.iata
FROM new_flight_small S
JOIN Airports P
ON P.iata = S.destination
LEFT JOIN Airlines L
ON L.carrier_code = S.carrier_code
WHERE P.iata IN (SELECT P.iata
FROM new_flight_small S
LEFT JOIN Airports P
ON P.iata = S.destination
LEFT JOIN Airlines L
ON L.carrier_code = S.carrier_code
WHERE (UNIX_TIMESTAMP(CONCAT('1970-01-01 ', actual_arrival_time), 'yyyy-MM-dd HHmm') -
    UNIX_TIMESTAMP(CONCAT('1970-01-01 ',  actual_departure_time), 'yyyy-MM-dd HHmm'))
    / 60 > (SELECT 
    MEAN((UNIX_TIMESTAMP(CONCAT('1970-01-01 ', actual_arrival_time), 'yyyy-MM-dd HHmm') -
    UNIX_TIMESTAMP(CONCAT('1970-01-01 ',  actual_departure_time), 'yyyy-MM-dd HHmm'))
    / 60)
FROM new_flight_small
))
GROUP BY P.iata
ORDER BY count(*) DESC
LIMIT 5)
GROUP BY P.iata, L.name
),

airline_per_airport AS(
SELECT iata, name, name_count 
FROM(SELECT iata, name, name_count, row_number() over(partition by iata order by name_count desc) as rn
FROM long_flights_airlines 
)A 
WHERE rn=1
)

SELECT X.iata as `Airport Code`, P.airport as `Airport Name`, long_flights_count_airport as `Number of Longflight Arrivals`, Z.name as `Airline Name with most Longflight Arrival`, Z.name_count as `Number of Longflight Arrivals of Airline`, mean(minutes) as `Average Longflight Duration`
FROM long_flights_per_airport X
LEFT JOIN airline_mins Y
ON X.iata == Y.iata
LEFT JOIN airline_per_airport Z
ON X.iata == Z.iata
LEFT JOIN Airports P
ON X.iata == P.iata
GROUP BY X.iata, long_flights_count_airport, Z.name, Z.name_count, P.airport


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Here is the code that I rewrote so that it works with Pgadmin due to my initial misunderstanding of the task as it wasn't needed but I thought it would be good to include it in here
-- MAGIC
-- MAGIC TLDR: I had to change my method of extracting the minutes to make it work in Postgres

-- COMMAND ----------

WITH new_flight_small AS (SELECT carrier_code, flight_number, flight_date, origin, destination, distance,
    CASE 
        WHEN actual_arrival_time = '2400' THEN '0000'
        ELSE actual_arrival_time
    END as actual_arrival_time,
	CASE 
        WHEN actual_departure_time = '2400' THEN '0000'
        ELSE actual_departure_time
    END as actual_departure_time		
FROM flights_small),


airline_mins AS (
SELECT P.iata, L.name, (EXTRACT(EPOCH FROM TO_TIMESTAMP('2000-01-01 ' || LPAD(actual_arrival_time::text, 4, '0'), 'yyyy-MM-dd HH24mi')) -
    EXTRACT(EPOCH FROM TO_TIMESTAMP('2000-01-01 ' || LPAD(actual_departure_time::text, 4, '0'), 'yyyy-MM-dd HH24mi')))
    / 60 AS minutes
FROM new_flight_small S
LEFT JOIN Airports P
ON P.iata = S.destination
LEFT JOIN Airlines L
ON L.carrier_code = S.carrier_code
WHERE (EXTRACT(EPOCH FROM TO_TIMESTAMP('2000-01-01 ' || LPAD(actual_arrival_time::text, 4, '0'), 'yyyy-MM-dd HH24mi')) -
    EXTRACT(EPOCH FROM TO_TIMESTAMP('2000-01-01 ' || LPAD(actual_departure_time::text, 4, '0'), 'yyyy-MM-dd HH24mi')))
    / 60 > (SELECT 
    AVG((EXTRACT(EPOCH FROM TO_TIMESTAMP('2000-01-01 ' || LPAD(actual_arrival_time::text, 4, '0'), 'yyyy-MM-dd HH24mi')) -
    EXTRACT(EPOCH FROM TO_TIMESTAMP('2000-01-01 ' || LPAD(actual_departure_time::text, 4, '0'), 'yyyy-MM-dd HH24mi')))
    / 60) FROM new_flight_small)),
    
long_flights_per_airport AS (
SELECT  P.iata, count(*) as long_flights_count_airport
FROM new_flight_small S
JOIN Airports P
ON P.iata = S.destination
LEFT JOIN Airlines L
ON L.carrier_code = S.carrier_code
WHERE P.iata IN (SELECT P.iata
FROM new_flight_small S
LEFT JOIN Airports P
ON P.iata = S.destination
LEFT JOIN Airlines L
ON L.carrier_code = S.carrier_code
WHERE (EXTRACT(EPOCH FROM TO_TIMESTAMP('2000-01-01 ' || LPAD(actual_arrival_time::text, 4, '0'), 'yyyy-MM-dd HH24mi')) -
    EXTRACT(EPOCH FROM TO_TIMESTAMP('2000-01-01 ' || LPAD(actual_departure_time::text, 4, '0'), 'yyyy-MM-dd HH24mi')))
    / 60 > (SELECT 
    AVG((EXTRACT(EPOCH FROM TO_TIMESTAMP('2000-01-01 ' || LPAD(actual_arrival_time::text, 4, '0'), 'yyyy-MM-dd HH24mi')) -
    EXTRACT(EPOCH FROM TO_TIMESTAMP('2000-01-01 ' || LPAD(actual_departure_time::text, 4, '0'), 'yyyy-MM-dd HH24mi')))
    / 60)
FROM new_flight_small)
)
GROUP BY P.iata
ORDER BY count(*) DESC
LIMIT 5),

long_flights_airlines AS (
SELECT P.iata, L.name, count(L.name) as name_count
FROM new_flight_small S
JOIN Airports P
ON P.iata = S.destination
LEFT JOIN Airlines L
ON L.carrier_code = S.carrier_code
WHERE P.iata in (SELECT P.iata
FROM new_flight_small S
JOIN Airports P
ON P.iata = S.destination
LEFT JOIN Airlines L
ON L.carrier_code = S.carrier_code
WHERE P.iata IN (SELECT P.iata
FROM new_flight_small S
LEFT JOIN Airports P
ON P.iata = S.destination
LEFT JOIN Airlines L
ON L.carrier_code = S.carrier_code
WHERE (EXTRACT(EPOCH FROM TO_TIMESTAMP('2000-01-01 ' || LPAD(actual_arrival_time::text, 4, '0'), 'yyyy-MM-dd HH24mi')) -
    EXTRACT(EPOCH FROM TO_TIMESTAMP('2000-01-01 ' || LPAD(actual_departure_time::text, 4, '0'), 'yyyy-MM-dd HH24mi')))
    / 60 > (SELECT 
    AVG((EXTRACT(EPOCH FROM TO_TIMESTAMP('2000-01-01 ' || LPAD(actual_arrival_time::text, 4, '0'), 'yyyy-MM-dd HH24mi')) -
    EXTRACT(EPOCH FROM TO_TIMESTAMP('2000-01-01 ' || LPAD(actual_departure_time::text, 4, '0'), 'yyyy-MM-dd HH24mi')))
    / 60)
FROM new_flight_small
))
GROUP BY P.iata
ORDER BY count(*) DESC
LIMIT 5)
GROUP BY P.iata, L.name
),

airline_per_airport AS(
SELECT iata, name, name_count 
FROM(SELECT iata, name, name_count, row_number() over(partition by iata order by name_count desc) as rn
FROM long_flights_airlines 
)A 
WHERE rn=1
)

SELECT X.iata as Airport_Code, P.airport as Airport_Name, long_flights_count_airport as Number_of_Longflight_Arrivals, Z.name as Airline_Name_with_most_Longflight_Arrival, Z.name_count as Number_of_Longflight_Arrivals_of_Airline, avg(minutes) as Average_Longflight_Duration
FROM long_flights_per_airport X
LEFT JOIN airline_mins Y
ON X.iata = Y.iata
LEFT JOIN airline_per_airport Z
ON X.iata = Z.iata
LEFT JOIN Airports P
ON X.iata = P.iata
GROUP BY X.iata, long_flights_count_airport, Z.name, Z.name_count, P.airport;
