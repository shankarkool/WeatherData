# TOY WEATHER SIMULATOR #

Toy weather simulator generates weather data for the given locations. It uses Machine Learning Techniques to predict the weather condition from the model

WeatherData.py        - Generates dataset for creating a model

WeatherSimulation.py  - Creates Linear & Logistic Regression model and Predicts the weather Condition for the given locations

#PLATFORM

These programs have been developed in Databricks cloud platform https://community.cloud.databricks.com

#REQUIREMENTS
- Input files for Training & Testing
- Software: Spark 2.1 Python 3
- Library: spark_mooc_meta for databricks cloud users. Forcastio API:  https://pypi.python.org/pypi/python-forecastio/
- SparkContext/SQLContext objects have to be created for standalone users to submit this spark code
- Google APIKEY is required to call geocode API
- Forcastio APIKey is required to call forcastioAPI
	  
#ENHANCEMENTS
- Accuracy of the prediction can be increased with huge dataset in supervised learning

#Exceptional Handling
- Incorrect Location
- API QueryLimit Exceeds
- API ERROR

#Unit Testing

https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4120395326559937/1595538202632760/8057212935484939/latest.html
