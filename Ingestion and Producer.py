import csv
from json import dumps
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: dumps(x).encode('utf-8'))
topic_name = 'projekDE'

with open('train.csv', 'r') as file:
    reader = csv.DictReader(file, delimiter = ',')
    for messages in reader:
        data = {}
        data["mean_profile"] = messages.get("mean_profile")
        data["sd_profile"] = messages.get("sd_profile")
        data["excess_profile"] = messages.get("excess_profile")
        data["skewness_profile"] = messages.get("skewness_profile")
        data["mean_curve"] = messages.get("mean_curve")
        data["sd_curve"] = messages.get("sd_curve")
        data["excess_curve"] = messages.get("excess_curve")
        data["skewness_curve"] = messages.get("skewness_curve")
        data["target_class"] = messages.get("target_class")
        print(data)
        producer.send(topic_name, data)
        producer.flush()