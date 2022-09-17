import paho.mqtt.client as mqtt
from questdb.ingress import Sender
from datetime import datetime
import json
import os

MQTT_TOPIC = [("/v1/data/+/+/", 0)]

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(MQTT_TOPIC)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
    string = msg.topic.split("/")
    json_payload = json.loads(msg.payload)
    dataType = string[4]
    with Sender(quest_db_ip, 9009) as sender:
        sender.row(
            'sensors_'+dataType,
            symbols={'device': json_payload['device'] , 'sensor': json_payload['sensor'] },
            columns={dataType: json_payload[dataType]},
            at=datetime.fromtimestamp(json_payload['time']) )
        sender.flush()

#clientdb = InfluxDBClient("10.128.0.50", 9009, '', '', 'test')
#clientdb.create_database('test')

mqtt_broker_ip = os.getenv('MQTT_BROKER_IP')
quest_db_ip = os.getenv('QUEST_DB_IP')


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(mqtt_broker_ip, 1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()

