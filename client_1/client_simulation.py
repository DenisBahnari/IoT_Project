import paho.mqtt.client as mqtt # type: ignore
import time
import csv
import json

BROKER_ADDRESS = "localhost"
BROKER_PORT = 8883
TOPIC_EV = "dataset/ev/online"
CSV_FILE = "client_1/dataset_ev/dataset-EV_with_stations_for_online_simulation.csv"

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker")
        client.subscribe(TOPIC_EV)
    else:
        print("Failed to connect, return code %d\n", rc)

client = mqtt.Client()
client.on_connect = on_connect

client.tls_set(ca_certs="client_1/certs/ca.crt")
client.username_pw_set("username", "senha123") # Valid credentials for simulation
client.connect(BROKER_ADDRESS, BROKER_PORT, 60)
client.loop_start()

def pub_ev_data():
    with open(CSV_FILE, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";")
        for row in reader:
            payload = json.dumps(row)
            client.publish(TOPIC_EV, payload)
            print(f"Published to {TOPIC_EV}: {payload}")
            time.sleep(2)

try:
    pub_ev_data()
except KeyboardInterrupt:
    print("Simulation interrupted...")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    client.loop_stop()
    client.disconnect()