import utils.mqtt_subscriber as MQTTSub
import utils.db as DB
import json
import csv
import requests

OFFLINE_DATA_FOLDER = "trainnning_dataset/"
DATASET_EV_FILE = "dataset-EV_with_stations.csv"
DATASET_STATIONS_FILE = "EV-Stations_with_ids_coords.csv"


def main():
    # Offline Data Loading
    print("Loading station data into memory...", flush=True)
    station_map = {}

    with open(OFFLINE_DATA_FOLDER + DATASET_STATIONS_FILE, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";")
        for row in reader:
            station_map[row["\ufeffStation ID"]] = row

    print(f"Loaded {len(station_map)} stations into memory.", flush=True)

    print("Inserting EV sessions into the database...", flush=True)
    with open(OFFLINE_DATA_FOLDER + DATASET_EV_FILE, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";")
        for row in reader:
            station_id = row["Charging Station ID"]
            station_row = station_map.get(station_id)
            if station_row:
                DB.insert_station_data(json.dumps(station_row))
            DB.insert_ev_data(json.dumps(row))

    print("Offline dataset loaded to database!", flush=True)

    # ML Processing
    url = "http://ml_processor:5000/train"
    payload = {"input_data": "Start training"}
    response = requests.post(url, json=payload)
    print("ML model training request sent to ML processor.", flush=True)
    print(response.status_code, flush=True)
    if response.status_code == 200:
        print("ML model training initiated successfully.", flush=True)

    # Online Data Processing
    mqqt_sub = MQTTSub.MqttSubscriber()
    mqqt_sub.connect()

    print("Subscribed to MQTT topic for online EV data...", flush=True)

    def on_message(client, userdata, msg):
        DB.insert_ev_data(msg.payload.decode())
        print("Online EV dataset sent to DB!", flush=True)

    mqqt_sub.client.on_message = on_message

    mqqt_sub.start()


if __name__ == "__main__":
    main()
