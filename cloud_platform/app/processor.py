import time
import utils.mqtt_subscriber as MQTTSub
import utils.mqtt_publisher as MQTTPub
import utils.db as DB
import json
import csv
import requests

OFFLINE_DATA_FOLDER = "trainnning_dataset/"
DATASET_EV_FILE = "dataset-EV_with_stations.csv"
DATASET_STATIONS_FILE = "EV-Stations_with_ids_coords.csv"


def update_dashboard_stats():
    print("Updating dashboard stats...", flush=True)
    mqtt_pub = MQTTPub.MqttPublisher()
    mqtt_pub.connect()

    trendsStats = DB.get_daily_weekly_monthly_trends()
    trendsStats = DB.make_json_safe(trendsStats)

    todDistribution = DB.get_time_of_day_distribution()
    todDistribution = DB.make_json_safe(todDistribution)

    totalStats = {
        "trendsStats": trendsStats,
        "todDistribution": todDistribution
    }
    mqtt_pub.publish(json.dumps(totalStats))
    print("Dashboard stats updated!", flush=True)


def main():
    # Offline Data Loading
    station_map = {}

    with open(OFFLINE_DATA_FOLDER + DATASET_STATIONS_FILE, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";")
        for row in reader:
            station_map[row["\ufeffStation ID"]] = row

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
    print("Trainning ML models...", flush=True)
    url = "http://ml_processor:5000/train"
    ev_sessions_data = DB.get_all_ev_sessions()
    ev_sessions_data = DB.make_json_safe(ev_sessions_data)
    payload = {"ev_sessions": ev_sessions_data}
    response = requests.post(url, json=payload)
    result = response.json()
    if response.status_code == 200:
        status = result["status"]
        meta = result["meta"]
        print("ML Models Trained |", end="", flush=True)
        print(f" Status: {status} |", end="", flush=True)
        print(f" Meta: {meta}", flush=True)
    else:
        error = result["error"]
        print(f"Erro: {error}")


    # Update Dashboard Stats
    update_dashboard_stats()


    # Online Data Processing
    url = "http://ml_processor:5000/predict_session"
    mqqt_sub = MQTTSub.MqttSubscriber()
    mqqt_sub.connect()

    print(flush=True)
    print("#############################", flush=True)
    print("Waiting for online EV data...", flush=True)
    print("#############################", flush=True)

    def on_message(client, userdata, msg):
        row = msg.payload.decode()
        rowjson = json.loads(row)
        station_id = rowjson.get("Charging Station ID")
        station_row = station_map[station_id]
        if station_row:
            DB.insert_station_data(json.dumps(station_row))
            DB.insert_ev_data(row)

        ev_session = DB.get_last_inserted_session()
        ev_session = DB.make_json_safe(ev_session)

        payload = {"ev_session": ev_session}
        response = requests.get(url, json=payload)
        result = response.json()
        
        if response.status_code == 200:
            status = result["status"]
            meta = result["result"]
            print(f" Status: {status} |", end="", flush=True)
            print(f" Result: {meta}", flush=True)
        else:
            error = result["error"]
            print(f"Erro: {error}")
        
        update_dashboard_stats()
        print("Online EV dataset sent to DB!", flush=True)
        

    mqqt_sub.client.on_message = on_message

    mqqt_sub.start()


if __name__ == "__main__":
    main()
