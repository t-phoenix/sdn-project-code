#"lat": 40.71,
#"lon": -74


import json 
from json import dumps
import requests
from kafka import KafkaProducer
from time import sleep
from datetime import datetime





producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

#for e in range(1000):
#   data = {'number' : e}
#    producer.send('numtest', value=data)
#    print(e)
#    sleep(2)

val1 = input("Enter longitude:")
val2 = input("Enter Latitude:")

parameters = {
    "lat": val1,
    "lon": val2
}


response = requests.get("http://api.open-notify.org/iss-pass.json", params=parameters)

status = response.status_code
print('status code:', status)
print(" ")

def jprint(obj):
    # create a formatted string of the Python JSON object
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)

pass_times = response.json()['response']
print("Next 5 ISS passing time :")
#jprint(pass_times)

data = {}
count=0
for d in pass_times:
    count+=1
    for k in d:
        s = k + str(count)
        data[s] = d[k]
print(data)



#print(response.json())
#jres=pass_times.json()
jres=json.dumps(data, sort_keys=True, indent =4)
producer.send('numtest',value=jres)
#print(jres)
sleep(1)








