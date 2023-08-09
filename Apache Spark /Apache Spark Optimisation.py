# Databricks notebook source
# MAGIC %md
# MAGIC # DATA3404 Assignment 2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use a modified verion of GoogleDriveDownloader.
# MAGIC The original version of this from pypi is broken due to a Google update. This pared-down version solves the issue for our use case.

# COMMAND ----------

## NOTE: The code in this chunk was provided
from __future__ import print_function
import requests
import zipfile
import warnings
from sys import stdout
from os import makedirs
from os.path import dirname
from os.path import exists


class GoogleDriveDownloader:
    CHUNK_SIZE = 32768
    DOWNLOAD_URL = 'https://docs.google.com/uc?export=download'

    @staticmethod
    def download_file_from_google_drive(file_id, dest_path, overwrite=False, unzip=False):
        destination_directory = dirname(dest_path)
        if not exists(destination_directory):
            makedirs(destination_directory)

        if not exists(dest_path) or overwrite:
            session = requests.Session()
            print('Downloading {} into {}... '.format(file_id, dest_path), end='')
            stdout.flush()
            params = {'id': file_id, 'confirm': "T"}
            response = session.get(GoogleDriveDownloader.DOWNLOAD_URL, params=params, stream=True)
            GoogleDriveDownloader._save_response_content(response, dest_path)
            print('Done.')

            if unzip:
                try:
                    print('Unzipping...', end='')
                    stdout.flush()
                    with zipfile.ZipFile(dest_path, 'r') as z:
                        z.extractall(destination_directory)
                    print('Done.')
                except zipfile.BadZipfile:
                    warnings.warn('Ignoring `unzip` since "{}" does not look like a valid zip file'.format(file_id))

    @staticmethod
    def _save_response_content(response, destination):
        with open(destination, 'wb') as f:
            for chunk in response.iter_content(GoogleDriveDownloader.CHUNK_SIZE):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)



# COMMAND ----------

# MAGIC %md
# MAGIC ### Retrieving data from Google Drive and upload them into Databricks FileStore.

# COMMAND ----------

## Note: The chunk in this chunk was provided
dbfs_fileStore_prefix = "/FileStore/tables"
prefix = "ontimeperformance"
files = [
  { "name": f"{prefix}_flights_small_cleaned.csv",   "file_id": "1faSgHJpVryJGygfTHeIEetymqFjXFz9m" },
  { "name": f"{prefix}_airlines.csv", "file_id": "1DdAQcr949xhfEgvNcCmqQQckbQASkzHQ" },
  { "name": f"{prefix}_airports.csv", "file_id": "1tOjrJpK7Tb_qn10hku-PG9Lhtei0CRhD" },
  { "name": f"{prefix}_aircrafts.csv", "file_id": "1Ab6uelyR9EdOgXCPM6oKaxWy5yJdHnVw" }
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove existing files

# COMMAND ----------

## Note: The chunk in this chunk was provided

import os
for file in files:
  if os.path.exists("/tmp/{}".format(file["name"])):
    os.remove("/tmp/{}".format(file["name"]))
  # dbutils.fs.rm("/FileStore/tables/{}".format(file["name"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Download datasets to your DBFS

# COMMAND ----------

## Note: The chunk in this chunk was provided

for file in files:
  GoogleDriveDownloader.download_file_from_google_drive(file_id=file["file_id"], dest_path="/tmp/{}".format(file["name"]))

# COMMAND ----------

## Note: The chunk in this chunk was provided

for file in files:
  dbutils.fs.mv("file:/tmp/{}".format(file["name"]), "/FileStore/tables/{}".format(file["name"]))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Check the files in Databricks FileStore.
# MAGIC You can use the below cell to explore the DBFS directories and check that the files are where they should be, and contain the correct data.

# COMMAND ----------

## Note: The chunk in this chunk was provided

display(dbutils.fs.ls("/FileStore/tables"))
display(dbutils.fs.head("/FileStore/tables/ontimeperformance_aircrafts.csv"))

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Aircrafts;
# MAGIC CREATE TABLE Aircrafts
# MAGIC USING csv
# MAGIC OPTIONS (path "/FileStore/tables/ontimeperformance_aircrafts.csv", header "true");
# MAGIC
# MAGIC DROP TABLE IF EXISTS Airlines;
# MAGIC CREATE TABLE Airlines
# MAGIC USING csv
# MAGIC OPTIONS (path "/FileStore/tables/ontimeperformance_airlines.csv", header "true");
# MAGIC
# MAGIC DROP TABLE IF EXISTS Airports;
# MAGIC CREATE TABLE Airports
# MAGIC USING csv
# MAGIC OPTIONS (path "/FileStore/tables/ontimeperformance_airports.csv", header "true");
# MAGIC
# MAGIC DROP TABLE IF EXISTS Flights_small;
# MAGIC CREATE TABLE Flights_small (
# MAGIC   carrier_code char(2), 
# MAGIC   flight_number int, 
# MAGIC   flight_date date, 
# MAGIC   origin char(3), 
# MAGIC   destination char(3), 
# MAGIC   tail_number varchar(10), 
# MAGIC   scheduled_depature_time char(4), 
# MAGIC   scheduled_arrival_time char(4), 
# MAGIC   actual_departure_time char(4), 
# MAGIC   actual_arrival_time char(4), 
# MAGIC   distance float)
# MAGIC USING csv
# MAGIC OPTIONS (path "/FileStore/tables/ontimeperformance_flights_small.csv", header "true");
# MAGIC
# MAGIC DROP TABLE IF EXISTS Flights_medium;
# MAGIC CREATE TABLE Flights_medium (
# MAGIC   carrier_code char(2), 
# MAGIC   flight_number int, 
# MAGIC   flight_date date, 
# MAGIC   origin char(3), 
# MAGIC   destination char(3), 
# MAGIC   tail_number varchar(10), 
# MAGIC   scheduled_depature_time char(4), 
# MAGIC   scheduled_arrival_time char(4), 
# MAGIC   actual_departure_time char(4), 
# MAGIC   actual_arrival_time char(4), 
# MAGIC   distance float)
# MAGIC USING csv
# MAGIC OPTIONS (path "/FileStore/tables/ontimeperformance_flights_medium.csv", header "true");
# MAGIC
# MAGIC DROP TABLE IF EXISTS Flights_large;
# MAGIC CREATE TABLE Flights_large (
# MAGIC   carrier_code char(2), 
# MAGIC   flight_number int, 
# MAGIC   flight_date date, 
# MAGIC   origin char(3), 
# MAGIC   destination char(3), 
# MAGIC   tail_number varchar(10), 
# MAGIC   scheduled_depature_time char(4), 
# MAGIC   scheduled_arrival_time char(4), 
# MAGIC   actual_departure_time char(4), 
# MAGIC   actual_arrival_time char(4), 
# MAGIC   distance float)
# MAGIC USING csv
# MAGIC OPTIONS (path "/FileStore/tables/ontimeperformance_flights_large.csv", header "true");
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
aircrafts_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("dbfs:/FileStore/tables/ontimeperformance_aircrafts.csv")
)
airlines_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("dbfs:/FileStore/tables/ontimeperformance_airlines.csv")
)
flights_s_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("dbfs:/FileStore/tables/ontimeperformance_flights_small.csv")
)
flights_m_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("dbfs:/FileStore/tables/ontimeperformance_flights_medium.csv")
)

airports_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("dbfs:/FileStore/tables/ontimeperformance_airports.csv")
)

flights_l_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("dbfs:/FileStore/tables/ontimeperformance_flights_large.csv")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Turning off Internal optimiser

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.shuffle.partitions", "1")
spark.conf.set("spark.sql.cbo.enabled", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task works by change by changing flights dataset between flights_s_df, flights_m_df, flights_l_df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Question 1: All codes were written by me
# MAGIC We were asked to rewrite a given SQL query with apache spark to determine the names of the top 5 US airlines with the most Boeing aircrafts, then optimised it.

# COMMAND ----------

# MAGIC %md
# MAGIC #####Unoptimised

# COMMAND ----------

##Original
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import year, date_format, col, expr, to_timestamp, format_string, sum,count
from sparkmeasure import StageMetrics


spark.sql("CLEAR CACHE").collect()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.shuffle.partitions", "1")
spark.conf.set("spark.sql.cbo.enabled", "false")
stagemetrics = StageMetrics(spark)
stagemetrics.begin()
joined_df = flights_l_df.join(airlines_df, ["carrier_code"])\
                        .join(aircrafts_df, aircrafts_df["tail_number"] == flights_l_df[" tail_number"])\
                        .filter(col("country") == "United States")\
                        .filter(col("manufacturer") == "BOEING")\
                        .groupby("name").count()\
                        .orderBy(col('count').desc())\
                        .limit(5).show()

stagemetrics.end()
stagemetrics.print_report()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Optimised Code 
# MAGIC
# MAGIC Done by broadcasting and projecting relevant columns in each dataset first before joining 

# COMMAND ----------

from pyspark.sql.functions import broadcast
from pyspark.sql.functions import year, date_format, col, expr, to_timestamp, format_string, sum,count
from sparkmeasure import StageMetrics

stagemetrics = StageMetrics(spark)
stagemetrics.begin()
spark.sql("CLEAR CACHE").collect()

aircrafts = broadcast(aircrafts_df.filter(col("manufacturer") == "BOEING").select("tail_number"))
airlines =broadcast(airlines_df.filter(col("country") == "United States").select("carrier_code", "name"))

df = flights_l_df.join(airlines, ["carrier_code"])\
                        .join(aircrafts, aircrafts["tail_number"] == flights_l_df[" tail_number"])\
                        .groupby("name").count()\
                        .orderBy(col('count').desc())\
                        .limit(5).show()
stagemetrics.end()
stagemetrics.print_report()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Question 2: Only the unoptimised code was written by me, the optimised version was done by my teammates
# MAGIC We were asked to determine the Airports with largest total delay, and their top 5 delayed airlines

# COMMAND ----------

# MAGIC %md
# MAGIC #####Unoptimised

# COMMAND ----------

from pyspark.sql.functions import year, date_format, col, expr, to_timestamp, format_string, sum,count, when
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum, row_number
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import concat, lit
from pyspark.sql.functions import collect_list, concat_ws

stagemetrics = StageMetrics(spark)
stagemetrics.begin()
spark.sql("CLEAR CACHE").collect()

def Result_list(flights,country,year):

  #Getting yyyy from flight date as a separate column
  flights_s_df = flights.withColumn('year', col(' flight_date').substr(1, 4))

  #Filters so that actual depart time is bigger than schedule to find delays. Pads the HHMM column with a 0 in front (ie 620--> 0620), calculates time_diff 
  delay_df = flights_s_df.filter(col(' actual_departure_time') >= col(' scheduled_depature_time'))\
                         .withColumn('padded_scheduled_depature_time', format_string('%04d', col(' scheduled_depature_time')))\
                         .withColumn('padded_actual_depature_time', format_string('%04d', col(' actual_departure_time')))\
                         .withColumn('scheduled_timestamp', to_timestamp(col('padded_scheduled_depature_time'), 'HHmm'))\
                         .withColumn('actual_timestamp', to_timestamp(col('padded_actual_depature_time'), 'HHmm'))\
                         .withColumn('time_diff_minutes', expr('int(actual_timestamp - scheduled_timestamp) / 60'))\
                         .dropna()

  # Join with airports df and filters by year and country. Grouped by country + summed delayed time
  airport_delay_df = delay_df.join(airports_df, delay_df[" origin"] == airports_df["airport_code"])\
                         .filter(col("year") == year).filter(col("country") == country)

  top10_df = airport_delay_df.groupBy("airport_name").agg(sum("time_diff_minutes").alias("total_delay"))\
                         .orderBy(expr("total_delay").desc())\
                         .limit(10)
  
  
  airport_name_list = top10_df.select("airport_name").rdd.flatMap(lambda x: x).collect()

  df = airlines_df.join(delay_df, "carrier_code")
  df = df.join(airports_df, df[" origin"] == airports_df["airport_code"])
  filtered_df = df.filter(col("airport_name").isin(airport_name_list))

  a = filtered_df.select("airport_name", "name", "time_diff_minutes")\
                 .filter(col("year") == year).filter(airports_df["country"] == "USA")\
                 .groupBy("airport_name", "name")\
                 .agg(sum("time_diff_minutes").alias("total_delay"))\
                 .orderBy("airport_name", sum("time_diff_minutes").desc())\
  
  window = Window.partitionBy("airport_name").orderBy(col("total_delay").desc())

  # Step 3: Create a rank column
  ranked_df = a.withColumn("rank", row_number().over(window))

  # Step 4: Filter to keep only the top 5 airlines for each airport
  top5_df = ranked_df.filter(col("rank") <= 5)
  
  airline_delay = top10_df.select("total_delay").orderBy("airport_name")
  total_delay_list = airline_delay.rdd.flatMap(lambda x: x).collect()
  
  ##IMPORTANT: The code below this line belongs to my teammates
  top5_df = top5_df.withColumn("id", monotonically_increasing_id())
  top5_with_percentage = top5_df.withColumn("percentage",
                                          when(top5_df["id"] <= 4, col("total_delay")/total_delay_list[0])
                                          .when((top5_df["id"] > 4) & (top5_df["id"] <= 9), col("total_delay")/total_delay_list[1])
                                          .when((top5_df["id"] > 9) & (top5_df["id"] <= 14), col("total_delay")/total_delay_list[2])
                                          .when((top5_df["id"] > 14) & (top5_df["id"] <= 19), col("total_delay")/total_delay_list[3])
                                          .when((top5_df["id"] > 19) & (top5_df["id"] <= 24), col("total_delay")/total_delay_list[4])
                                          .when((top5_df["id"] > 24) & (top5_df["id"] <= 29), col("total_delay")/total_delay_list[5])
                                          .when((top5_df["id"] > 29) & (top5_df["id"] <= 34), col("total_delay")/total_delay_list[6])
                                          .when((top5_df["id"] > 34) & (top5_df["id"] <= 39), col("total_delay")/total_delay_list[7])
                                          .when((top5_df["id"] > 39) & (top5_df["id"] <= 44), col("total_delay")/total_delay_list[8])
                                          .when((top5_df["id"] > 44) & (top5_df["id"] <= 49), col("total_delay")/total_delay_list[9]))
  results = top5_with_percentage.withColumn("percentage",
                                   concat((col("percentage") * 100).cast("int"), lit("%")))
  
  
  final_results = results.withColumn("airlines", concat(lit("("), results["name"], lit(", "), results["percentage"], lit(")")))
  final_results = final_results.drop("name","total_delay","rank","id","percentage")

  final_results = final_results.groupBy("airport_name").agg(concat_ws(",", collect_list("airlines")).alias("airlines"))
  final_results = final_results.join(top10_df.select('airport_name','total_delay'), on='airport_name')
  
  final = final_results.withColumn("output", concat(
    col("airport_name"), lit("\t"),
    col("total_delay"), lit("\t"),
    col("airlines")))

  final = final.select("output")

  return final
  

# Result_list(flights_s_df,"USA", 2004).show(truncate=False)
df = Result_list(flights_s_df,"USA", 2004).show(truncate=False)
stagemetrics.end()
stagemetrics.print_report()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Optimised

# COMMAND ----------

## Important: Not written by me
# Use window function
from pyspark.sql.window import Window
from pyspark.sql.functions import col, concat, lit, round,struct
stagemetrics = StageMetrics(spark)
stagemetrics.begin()
spark.sql("CLEAR CACHE").collect()

def Opt_Result_list_2(flights,country, year):
  #Getting yyyy from flight date as a separate column
  flights_df = flights.withColumn('year', col(' flight_date').substr(1, 4))
  
  flights_df = flights_df.filter(col("year") == year)

  #Filters so that actual depart time is bigger than schedule to find delays. Pads the HHMM column with a 0 in front (ie 620--> 0620), calculates time_diff 
  delay_df = flights_df.filter(col(' actual_departure_time') > col(' scheduled_depature_time'))\
                         .withColumn('padded_scheduled_depature_time', format_string('%04d', col(' scheduled_depature_time')))\
                         .withColumn('padded_actual_depature_time', format_string('%04d', col(' actual_departure_time')))\
                         .withColumn('scheduled_timestamp', to_timestamp(col('padded_scheduled_depature_time'), 'HHmm'))\
                         .withColumn('actual_timestamp', to_timestamp(col('padded_actual_depature_time'), 'HHmm'))\
                         .withColumn('time_diff_minutes', expr('int(actual_timestamp - scheduled_timestamp) / 60'))\
                         .dropna()
  
  delay_df = delay_df.select(col("carrier_code"), col(" origin"), col("time_diff_minutes"))
  
  
  airports_df_filter = airports_df.filter(col("country") == country).select(col("airport_code"), col("airport_name"))

  # Join with airports df with delay df
  airport_delay_df = delay_df.join(airports_df_filter, delay_df[" origin"] == airports_df_filter["airport_code"]).drop(col(" origin"))
  
  # Calculate airport total delay
  windowSpec = Window.partitionBy('airport_name')
  airport_delay_df = airport_delay_df.withColumn('airport_delay', F.sum('time_diff_minutes').over(windowSpec))
  
  # Get airline df
  airlines_df_select = airlines_df.select(col("carrier_code"), col("name"))
  
  # Join delay df with airline df
  airline_airport_delay_df = airport_delay_df.join(airlines_df, "carrier_code")
  
  # Calculate airline total delay at each airport
  windowSpec = Window.partitionBy("airport_name", "airport_delay", "name")
  airline_airport_delay_df = airline_airport_delay_df.withColumn('airline_delay', F.sum('time_diff_minutes').over(windowSpec))
  
  # Select useful columns with a distinct row
  airline_airport_delay_df = airline_airport_delay_df.select(col("airport_name"), col("airport_delay"), col("name"), col("airline_delay")).distinct()
  
  # Sort by airport total delay and airline total delay at that airport
  airline_airport_delay_df = airline_airport_delay_df.orderBy(F.desc("airport_delay"), F.desc("airline_delay"))
  
  # Compute percentage of airlines' delay contribution
  airline_airport_delay_df = airline_airport_delay_df.withColumn("airline_delay_percent", concat(round(col("airline_delay")/col("airport_delay")*100, 2), lit("%")) )
  
  # Construct airline name and percentage to a tuple
  airline_airport_delay_df = airline_airport_delay_df.withColumn("airline_tuple", struct("name", "airline_delay_percent").cast("string"))\
                                                     .drop("airline_delay", "name", "airline_delay_percent")\
    
  # Collect airline tuples into list grouped by airport name                                    
  airline_airport_delay_df = airline_airport_delay_df.groupBy("airport_name", "airport_delay")\
                                                     .agg(collect_list("airline_tuple").alias("airlines_lis"))\
           
  # Slice the top 5 airlines
  airline_airport_delay_df = airline_airport_delay_df.select("airport_name", col("airport_delay").alias("total_departure_delay"), expr("slice(airlines_lis, 1, 5)").alias("top5_airlines_delay"))
                                                    
  return airline_airport_delay_df.orderBy(F.desc(col("airport_delay"))).limit(10)

df = Opt_Result_list_2(flights_s_df,"USA", 2004).show(truncate=False)

stagemetrics.end()
stagemetrics.print_report()
