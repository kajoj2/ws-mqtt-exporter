import paho.mqtt.client as mqtt
from questdb.ingress import Sender
from datetime import datetime
import json
import os

MQTT_TOPIC = [("/v1/data/+/+/", 0)]

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
    string = msg.topic.split("/")
    json_payload = json.loads(msg.payload)
    dataType = string[4]

    sender.row(
        'sensors_'+dataType,
        symbols={'device': json_payload['device'] , 'sensor': json_payload['sensor'] },
        columns={dataType: json_payload[dataType]},
        at=datetime.fromtimestamp(json_payload['time']))
    sender.flush()

mqtt_broker_ip = os.getenv('MQTT_BROKER_IP')
quest_db_ip = os.getenv('QUEST_DB_IP')

with Sender(quest_db_ip, 9009) as sender:
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(mqtt_broker_ip, 1883, 60)

    client.loop_forever()

