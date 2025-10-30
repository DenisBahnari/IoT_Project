import mqtt_subscriber as MQTTSub
import csv

OFFLINE_DATA_FOLDER = "../data/trainning_dataset/"
DATASET_EV_FILE = "dataset-EV_with_stations.csv"
DATASET_STATIONS_FILE = "EV-Stations_with_ids_coords.csv"

# Ofline Data Loading
with open(OFFLINE_DATA_FOLDER + DATASET_EV_FILE, newline="", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile, delimiter=";")
    for row in reader:
        pass  # Send data do DB and process it

with open(OFFLINE_DATA_FOLDER + DATASET_STATIONS_FILE, newline="", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile, delimiter=";")
    for row in reader:
        pass  # Send data do DB and process it


# Online Data Processing
mqqt_sub = MQTTSub.MqttSubscriber()
mqqt_sub.connect()
mqqt_sub.start()
received_message = mqqt_sub.on_message()
# Send data do DB and process it



