# Databricks notebook source
"""
TOY WEATHER SIMULATION  - WeatherSimulation.py

This Program uses Machine Learning Techniques to predict/generate the data for weather simulation

PLATFORM
This code has been developed in Databricks cloud platform https://community.cloud.databricks.com

REQUIREMENTS
      - Input files for Supervised Learning and TestData Refer InputFolder
      - Software: Spark 2.1 Python 3
      - Library: spark_mooc_meta for databricks cloud users
      - SparkContext/SQLContext objects has to be created for standalone users to submit this spark code
      - Google APIKEY is required to call geocode API
"""
from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.types import *
from datetime import *
from pyspark.sql.functions import concat,lit,col
import requests

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

#Generates sequential Date&Time for every Month. This can also be set to RandomGenerator 

def get_datetime_info(geo_info):
  start_date=datetime(2016, 1, 1)
  for date_offset in range(0, 365, 30):
    new_date=start_date+timedelta(date_offset)
    LocalTime = int((new_date).strftime('%s'))
    yield [geo_info[0],float(geo_info[1]),float(geo_info[2]),float(geo_info[3]),int(LocalTime)]

# COMMAND ----------

"""
The Following code Creates a pipeline model with Linear & Logistic Regression Techniques

TrainSource 
    - File contains Historical data with Latitude, Longitude, Elevation, LocalTime, Temperature, Pressure, Humid, Condition
    - Features: Latitude, Longitude, Elevation, LocalTime
    - Label: Temperature, Pressure, Humid, Condition

Linear Regression
    - With the Features and Labels, It creates a Modal for finding Temperature, Pressure, Humid with Latitude, Longitude, Elevation, LocalTime as Input.

Logistic Regression
    - With the Features and Labels, It creates a Modal for finding Conditions(Rain,Snow,Sunny) with Latitude, Longitude, Elevation, LocalTime as Input.
"""

#Source and Result FileNames
TrainSource ="dbfs:/mnt/awshank/Test/training_results.csv"
TestSource="dbfs:/mnt/awshank/Test/target_locations.txt"
TestResult ="dbfs:/mnt/awshank/Test/target_results.psv"

#Condition Expression
ConditionExpr = "CASE WHEN Cond LIKE \'%Snow%\' THEN 0 WHEN Cond LIKE \'%Rain%\' THEN 1 ELSE 2 END AS Condition"
ConditionRevExpr = ["Snow","Rain","Sunny"]

#Read Source file and creates DataFrame
ModalDF = sqlContext.read.csv(TrainSource,header="True",inferSchema="True").selectExpr("*",ConditionExpr)

#Transforms Input columns into single ArrayList called features
vectorizer = VectorAssembler()
vectorizer.setInputCols(["Lat", "Long", "Ele","LocalTime"])
vectorizer.setOutputCol("features")

#Declaring objects for Each Regressions
lr0 = LogisticRegression(labelCol="Condition",predictionCol="Predicted_Cond",maxIter=100, regParam=0, family="multinomial")
lr1 = LinearRegression(labelCol="Temp",predictionCol="Predicted_Temp",maxIter=100,regParam=0.1)
lr2 = LinearRegression(labelCol="Pres",predictionCol="Predicted_Pres",maxIter=100,regParam=0.1)
lr3 = LinearRegression(labelCol="Humid",predictionCol="Predicted_Humid",maxIter=100,regParam=0.1)

#Combining all the Regression in a pipeline and fit the Dataset to create a Modal
lrPipeline = Pipeline()
lrPipeline.setStages([vectorizer, lr1, lr2,lr3,lr0])
lrModel = lrPipeline.fit(ModalDF)

# COMMAND ----------

"""
The Following code Take the Test Dataset and perform following actions
    - Gets GeoInformation (Latitude, Longitude,Elevation)
    - Gets Monthly Data&Timestamps for each record
    - Predict the Temperature, Pressure, Humidity & Condition using Pipeline Model
    - Change the Fomat and write it in a file
"""

# Extract and Transform TestRDD
TestRDD = sc.textFile(TestSource).map(lambda line: list(get_geo_info(line))).flatMap(lambda line: list(get_datetime_info(line)))

# Schema for DataFrame
schema = StructType([
  StructField("Location", StringType(), False),
  StructField("Lat", DoubleType(), False),
  StructField("Long", DoubleType(), False),
  StructField("Ele", DoubleType(), False),
  StructField("LocalTime", IntegerType(), False)
])
TestDF = sqlContext.createDataFrame(TestRDD, schema)

#Format change as per the standards
FormatTime  = udf(lambda x: datetime.fromtimestamp(x).isoformat(), StringType())
FormatTemp  = udf(lambda x:(x-32)*(5.0/9.0),DoubleType())
FormatHumid = udf(lambda x:int(x*100),IntegerType())
FormatCondition = udf(lambda x: ConditionRevExpr[int(x)], StringType())

TestTransformedDF=lrModel.transform(TestDF)\
                  .select(col('Location'),\
                   concat(col('Lat'),lit(','),col('Long'),lit(','),col('Ele')),\
                   FormatTime(col('LocalTime')),\
                   FormatCondition(col('Predicted_Cond')),\
                   FormatTemp(col('Predicted_Temp')),\
                   col('Predicted_Pres'),\
                   FormatHumid(col('Predicted_Humid')))

#Write the results in psv format
TestTransformedDF.repartition(1).write.csv(TestResult,mode="overwrite",sep="|")
