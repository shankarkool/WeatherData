# Databricks notebook source
"""
TOY WEATHER SIMULATION  - WeatherData.py

This Program is used to create a dataset for the weather simulation.

PLATFORM
This code has been developed in Databricks cloud platform https://community.cloud.databricks.com

REQUIREMENTS
      - Input file with Location name
      - Software: Spark 2.1 Python 3
      - Library: spark_mooc_meta for databricks cloud users. Forcastio:  https://pypi.python.org/pypi/python-forecastio/
      - SparkContext/SQLContext objects has to be created for standalone users to submit this spark code
      - Google APIKey is required to call geocodeAPI
      - Forcastio APIKey is required to call forcastioAPI
"""
import requests,forecastio
from datetime import datetime,timedelta
from pyspark.sql.types import *

#Gets Geo Information(Latitude, Longitude,Elevation) from GoogleAPI

def get_geo_info(city):
  geocode_url = 'https://maps.googleapis.com/maps/api/geocode/json' 
  geocode_api_key = 'AIzaSyApzw_jnCwzh56U2e_1lp_UZoXxbdCUFz8'
  geocode_params = {'sensor': 'false', 'address': city,'key': geocode_api_key}
  #location = requests.get(geocode_url, params=geocode_params).json()['results'][0]['geometry']['location']
  try:
    response = requests.get(geocode_url, params=geocode_params).json()
    location = response['results'][0]['geometry']['location']
  except:
    if response['status'] == 'ZERO_RESULTS':
      raise Exception("Invalid City Name: "+city)
    elif response['status'] == 'OVER_QUERY_LIMIT':
      raise Exception("Query Limit Exceeded")
    else:
      raise Exception("UNKNOWN ERROR")
  latitude = location['lat']
  longitude = location['lng']
  elevation_url = 'https://maps.googleapis.com/maps/api/elevation/json'
  elevation_api_key = 'AIzaSyB3Z2kw-drv4nEIFP-dkf6I95yX2SVeHrw' 
  elevation_params = {'sensor': 'false', 'locations': (str(latitude)+','+str(longitude)),'key': elevation_api_key}
  elevation = requests.get(elevation_url, params=elevation_params).json()['results'][0]['elevation']
  
  return (city,latitude,longitude,elevation)

#Gets Weather Information(Temperature,Pressure,Humidity) from ForcastioAPI

def get_weather_info(geo_info):
  api_key='e8b76a0a102f49d3e95de3015e9aaefb' 
  start_date=datetime(2015, 1, 1)
  for date_offset in range(0, 365, 7):
    forecast = forecastio.load_forecast(api_key,geo_info[1],geo_info[2],time=start_date+timedelta(date_offset),units="us")
    for hour in forecast.hourly().data:
      yield [geo_info[0],float(geo_info[1]),float(geo_info[2]),float(geo_info[3]),int(hour.d['time']),hour.d.get('summary', ''),float(hour.d.get('temperature', 50)),float(hour.d.get('pressure', 1000)),float(hour.d.get('humidity', 0.5))]

# COMMAND ----------

"""
The Following code Take the Train Dataset and perform following actions
    - Gets GeoInformation (Latitude, Longitude,Elevation)
    - Gets WeatherInformation (Temperature,Pressure,Humidity) on Hourly basis
    - Writes into a file in CSV Format
"""
#Source and Result FileNames
trainSource = "dbfs:/mnt/awshank/Test/training_locations.txt"
trainResult ="dbfs:/mnt/awshank/Test/training_results.csv"

#Extract and Transform RDDs
GeodataRDD = sc.textFile(trainSource).map(lambda line: list(get_geo_info(line)))
WeatherdataRDD = GeodataRDD.flatMap(lambda line:list(get_weather_info(line)))

#Schema for DataFrame
schema = StructType([
  StructField("Location", StringType(), False),
  StructField("Lat", DoubleType(), False),
  StructField("Long", DoubleType(), False),
  StructField("Ele", DoubleType(), False),
  StructField("LocalTime", IntegerType(), False),
  StructField("Cond", StringType(), False),
  StructField("Temp", DoubleType(), False),  
  StructField("Pres", DoubleType(), False),
  StructField("Humid", DoubleType(), False)
])
TrainDF = sqlContext.createDataFrame(WeatherdataRDD, schema)

#Write the Result in CSV Format
TrainDF.repartition(1).write.csv(trainResult,header="True",mode="overwrite")
