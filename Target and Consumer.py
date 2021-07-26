#Insialize Library
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from kafka import KafkaConsumer,TopicPartition
from json import loads
import json
import pandas as pd
import numpy as np
import sys

#Create Spark Session
spark = SparkSession \
  .builder \
  .appName("reading csv") \
  .config(
    "spark.driver.extraClassPath", 
    "C:\Spark\spark-3.0.1-bin-hadoop2.7\jars\mysql-connector-java-8.0.21.jar") \
  .getOrCreate()

#Inisialize Kafka Variable
topic_name = "projekDE"
kafka_server = "localhost:9092"
tp = TopicPartition(topic_name,0)

#Read Data From Topic
consumer = KafkaConsumer(
  group_id ='projek',
  bootstrap_servers=kafka_server,
  enable_auto_commit=True,
  value_deserializer=lambda x: json.loads(x.decode('utf-8'))
  )

#Inisialize Variabel to Make a Break When We Iterate The Data in message consumer
consumer.assign([tp])
consumer.seek_to_beginning(tp)  
lastOffset = consumer.end_offsets([tp])[tp]

#Extract Topic Data And Put It In Pandas Dataframe
column = [
  "mean_profile","sd_profile","excess_profile",
  "skewness_profile","mean_curve","sd_curve",
  "excess_curve","skewness_curve","target_class"]
df = pd.DataFrame(columns=column)
for message in consumer:
  output_message = message.value
  df =  df.append(output_message,ignore_index=True)
  if message.offset == lastOffset - 1:
    break

#Replace empty value with nan
for x in column:
  df[x].replace('', np.nan, inplace=True)

#Change Schema
df = df.astype(float) 

#Check
print(df.head(5))

#Create Pyspark Dataframe
dfSpark = spark.createDataFrame(df)

#Remove each row that have empty value 
columns = dfSpark.schema.names
for column in columns:
  print(column)
  dfSpark = dfSpark.na.drop(how='any', subset=column)


#Check Pt.2
print(dfSpark.show(5))

#Check the schema
dfSpark.printSchema()


#Put it to database
dfSpark.write.format('jdbc').options(
    url='jdbc:mysql://localhost/data_engineer?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',
    driver='com.mysql.cj.jdbc.Driver',
    dbtable='pulsar_star',
    user='root',
    password='').mode('append').save()

#Take it out
pulsar_star = spark.read.format("jdbc").options(
    url='jdbc:mysql://localhost/data_engineer?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',
    driver='com.mysql.cj.jdbc.Driver',
    dbtable='pulsar_star',
    user='root',
    password="").load()

#Convert it to csv file
pulsar_star.write.csv('pulsar_star.csv')

sys.exit()