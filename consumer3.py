from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads

consumer = KafkaConsumer(
    'numtest',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group3',
     value_deserializer=lambda x: loads(x.decode('utf-8')))



data = []
from flask import Flask
app = Flask(__name__)

@app.route('/')
def index():
    for message in consumer:
        data_curr = (message.value)
        data.append(data_curr)
        break
    st = ""
    for i in data:
        to_add = "<p>"
        for i in range(40):
            to_add += str(i)
        #to_add += str(0) + ", " + str(i[1]) 
        to_add += "</p>"
        st += to_add

    return ('<html><head><meta http-equiv="refresh" content="5" ></head><body><h1>'+st+'</h1></body></html>')

if __name__ == '__main__':
   app.run()