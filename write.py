import os
from datetime import datetime
from json import dumps, loads
from time import sleep
from random import randint
from kafka import KafkaConsumer, KafkaProducer

topic = 'recitation-c' # x could be b, c, d, e, f

# Create a producer to write data to kafka
# Ref: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html

# [TODO]: Replace '...' with the address of your Kafka bootstrap server
producer = KafkaProducer(bootstrap_servers=["localhost:9092"],
                        value_serializer=lambda x: dumps(x).encode('utf-8'))

# [TODO]: Add cities of your choice
cities = ["Mumbai", "Pune", "Chicago", "Pittsburgh", "Boston", "Moline"]
cities = ["New York", "Tokyo", "London", "Paris", "Sydney"]

# Write data via the producer
producer.send(topic=topic, value="hey its prachi")
# print("Writing to Kafka Broker")
# for i in range(0):
#     data = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")},{cities[randint(0,len(cities)-1)]},{randint(18, 32)}ºC'
#     print(f"Writing: {data}")
#     producer.send(topic=topic, value=data)
#     sleep(1)