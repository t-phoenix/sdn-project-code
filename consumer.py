from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads

consumer = KafkaConsumer(
    'numtest',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group1',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

#for message in consumer:
#    message = message.value
#    print('{} added to {}'.format(message, 1))

for message in consumer:
	print(message.value)
	