{\rtf1\ansi\ansicpg1252\cocoartf2639
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\paperw11900\paperh16840\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\pard\tx566\tx1133\tx1700\tx2267\tx2834\tx3401\tx3968\tx4535\tx5102\tx5669\tx6236\tx6803\pardirnatural\partightenfactor0

\f0\fs24 \cf0 set search_path to data3404agm1; \
DROP TABLE IF EXISTS aircrafts;\
CREATE TABLE aircrafts(\
	tailnum text,\
	type text,\
	manufacturer text,\
	issue_date date,\
	model text,\
	status text,\
	aircraft_type text,\
	engine_type text,\
	year integer\
);\
\
-- \
DROP TABLE IF EXISTS airlines;\
\
CREATE TABLE airlines(\
	carrier_code text,\
	name text,\
	country text\
);\
\
--\
DROP TABLE IF EXISTS airports;\
\
CREATE TABLE airports(\
	iata text, \
	airport text,\
	city text,\
	state text,\
	country text,\
	lat float,\
	long float\
);\
\
--\
DROP TABLE IF EXISTS Flights_large;\
\
CREATE TABLE Flights_large (\
  carrier_code text, \
  flight_number int, \
  flight_date date, \
  origin char(3), \
  destination char(3), \
  tail_number varchar(10), \
  scheduled_depature_time integer, \
  scheduled_arrival_time integer, \
  actual_departure_time integer, \
  actual_arrival_time integer, \
  distance float\
);\
  \
--\
DROP TABLE IF EXISTS Flights_medium;\
\
CREATE TABLE Flights_medium (\
  carrier_code text, \
  flight_number int, \
  flight_date date, \
  origin char(3), \
  destination char(3), \
  tail_number varchar(10), \
  scheduled_depature_time integer, \
  scheduled_arrival_time integer, \
  actual_departure_time integer, \
  actual_arrival_time integer, \
  distance float\
);\
\
--\
DROP TABLE IF EXISTS Flights_small;\
\
CREATE TABLE Flights_small (\
  carrier_code text, \
  flight_number integer, \
  flight_date date, \
  origin char(3), \
  destination char(3), \
  tail_number varchar(10), \
  scheduled_depature_time integer, \
  scheduled_arrival_time integer, \
  actual_departure_time integer, \
  actual_arrival_time integer, \
  distance float\
);\
 \
  \
}