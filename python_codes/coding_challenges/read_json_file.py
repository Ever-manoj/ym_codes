import json
from datetime import datetime


with open('sample.json')as f:
    data = json.load(f)
print(data)

dt_object = datetime.fromtimestamp(data['data'][0]['time'])
format_date = dt_object.strftime('%Y-%m-%d %H:%M:%S')
print()
print("Symbol :",data['config'][0]['symbol'])
print("Name :",data['data'][0]['name'])
print("Price :",data['data'][0]['price'])
print("percent_change_24 :",data['data'][0]['percent_change_24h'])
print("Time (%Y-%m-%d %H:%M:%S) :",format_date)
