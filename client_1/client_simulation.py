from datetime import datetime, timedelta 
import random
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
    with open(CSV_FILE, newline="", encoding="utf-8-sig") as csvfile:  
        reader = csv.DictReader(csvfile, delimiter=";")
        for row in reader:
            charging_duration = float(row.get("Charging Duration (hours)", 0))
            

            random_day = random.randint(60, 70)
            base_date = datetime(2024, 1, 1) + timedelta(days=random_day-1) 
            

            random_hour = random.randint(6, 23)
            random_minute = random.randint(0, 59)
            
            start_time = base_date.replace(hour=random_hour, minute=random_minute)
            end_time = start_time + timedelta(hours=charging_duration)  
            
            start_time_str = start_time.strftime("%d/%m/%y %H:%M")
            end_time_str = end_time.strftime("%d/%m/%y %H:%M")
            
            row_with_user_id = {
                "\ufeffUser ID": "User_1",
                **row,
                "Charging Start Time": start_time_str,
                "Charging End Time": end_time_str
            }
            
            payload = json.dumps(row_with_user_id)
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