from flask import Flask, request, jsonify # type: ignore
from sklearn.cluster import KMeans, DBSCAN # type: ignore
from sklearn.ensemble import IsolationForest # type: ignore
from sklearn.preprocessing import StandardScaler # type: ignore
from datetime import datetime
from waitress import serve # type: ignore
import pandas as pd
import numpy as np
import joblib # type: ignore
import json
import os

app = Flask(__name__)

MODEL_DIR = "models"

SESSION_CLUSTERS = 4          # KMeans clusters
DBSCAN_EPS = 0.5
DBSCAN_MIN_SAMPLES = 5
IFOREST_CONTAM = 0.05         # 5% anomalies

LIST_SCHEMA = [
    "idx",
    "User ID",
    "Vehicle Model",
    "Battery Capacity (kWh)",
    "Charging Station ID",
    "Charging Start Time",
    "Charging End Time",
    "Energy Consumed (kWh)",
    "Charging Duration (hours)",
    "Charging Rate (kW)",
    "Charging Cost (EUR)",
    "Time of Day",
    "Day of Week",
    "State of Charge (Start %)",
    "State of Charge (End %)",
    "Distance Driven (since last charge) (km)",
    "Temperature (C)",
    "Vehicle Age (years)"
]


def row_from_raw(raw):
    """Accept dict or list → normalized dict."""
    if isinstance(raw, dict):
        return raw

    if isinstance(raw, (list, tuple)):
        out = {}
        for i, key in enumerate(LIST_SCHEMA):
            out[key] = raw[i] if i < len(raw) else None
        return out
    
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except:
            return {}
    
    return {}

def safe_float(v):
    try:
        return float(v)
    except:
        return np.nan

def parse_datetime(v):
    try:
        return pd.to_datetime(v)
    except:
        return None


def featurize_session(r):
    start = parse_datetime(r.get("Charging Start Time"))
    end   = parse_datetime(r.get("Charging End Time"))

    battery_kwh = safe_float(r.get("Battery Capacity (kWh)"))
    energy = safe_float(r.get("Energy Consumed (kWh)"))
    duration_h = safe_float(r.get("Charging Duration (hours)"))
    rate_kw = safe_float(r.get("Charging Rate (kW)"))
    cost = safe_float(r.get("Charging Cost (EUR)"))
    soc_s = safe_float(r.get("State of Charge (Start %)"))
    soc_e = safe_float(r.get("State of Charge (End %)"))
    dist = safe_float(r.get("Distance Driven (since last charge) (km)"))
    temp = safe_float(r.get("Temperature (C)"))
    age  = safe_float(r.get("Vehicle Age (years)"))

    soc_delta = np.nan
    if not np.isnan(soc_s) and not np.isnan(soc_e):
        soc_delta = soc_e - soc_s

    energy_rel = np.nan
    if battery_kwh and battery_kwh > 0 and not np.isnan(energy):
        energy_rel = energy / battery_kwh

    hour = start.hour if start is not None else np.nan

    bin = None
    if not np.isnan(hour):
        h = int(hour)
        if 5 <= h < 11:    bin = "morning"
        elif 11 <= h < 15: bin = "midday"
        elif 15 <= h < 19: bin = "afternoon"
        elif 19 <= h < 23: bin = "evening"
        else:              bin = "night"

    intensity = np.nan
    if duration_h and duration_h > 0 and not np.isnan(energy):
        intensity = energy / duration_h

    return {
        "energy_kwh": energy,
        "duration_h": duration_h,
        "rate_kw": rate_kw,
        "cost_eur": cost,
        "soc_start": soc_s,
        "soc_end": soc_e,
        "soc_delta": soc_delta,
        "distance_km": dist,
        "temp_c": temp,
        "vehicle_age": age,
        "hour": hour,
        "time_bin": bin,
        "energy_rel": energy_rel,
        "intensity": intensity,
        "vehicle_model": r.get("Vehicle Model"),
        "user_id": r.get("User ID"),
        "station_id": r.get("Charging Station ID")
    }

SESSION_NUM_COLS = [
    "energy_kwh",
    "duration_h",
    "rate_kw",
    "cost_eur",
    "soc_start",
    "soc_end",
    "soc_delta",
    "distance_km",
    "temp_c",
    "vehicle_age",
    "hour",
    "energy_rel",
    "intensity",
]

def train_models(raw_rows):
    feat_rows = [featurize_session(row_from_raw(r)) for r in raw_rows]
    df = pd.DataFrame(feat_rows)

    df["bin_morning"] = (df["time_bin"] == "morning").astype(int)
    df["bin_midday"] = (df["time_bin"] == "midday").astype(int)
    df["bin_afternoon"] = (df["time_bin"] == "afternoon").astype(int)
    df["bin_evening"] = (df["time_bin"] == "evening").astype(int)
    df["bin_night"] = (df["time_bin"] == "night").astype(int)

    full_cols = SESSION_NUM_COLS + [
        "bin_morning", "bin_midday", "bin_afternoon", "bin_evening", "bin_night"
    ]

    df = df[full_cols].fillna(0)

    # SCALE
    scaler = StandardScaler()
    X = scaler.fit_transform(df.values)

    # K-MEANS
    kmeans = KMeans(n_clusters=SESSION_CLUSTERS, random_state=42, n_init=10)
    kmeans.fit(X)

    # DBSCAN
    dbscan = DBSCAN(eps=DBSCAN_EPS, min_samples=DBSCAN_MIN_SAMPLES)
    dbscan.fit(X)

    # ISOLATION FOREST
    iso = IsolationForest(contamination=IFOREST_CONTAM, random_state=42)
    iso.fit(X)

    trainning_results = {
        "scaler": scaler,
        "kmeans": kmeans,
        "dbscan": dbscan,
        "isolation": iso,
        "columns": full_cols
    }
    joblib.dump(trainning_results, os.path.join(MODEL_DIR, "session_models.pkl"))

    meta = {
        "n_sessions": len(df),
        "kmeans_clusters": SESSION_CLUSTERS,
        "dbscan_eps": DBSCAN_EPS,
        "iforest_contam": IFOREST_CONTAM
    }

    with open(os.path.join(MODEL_DIR, "meta.json"), "w") as f:
        json.dump(meta, f)

    return meta


def predict_session(raw):
    d = row_from_raw(raw)
    f = featurize_session(d)

    # time-bin encoding
    f["bin_morning"] = 1 if f["time_bin"]=="morning" else 0
    f["bin_midday"] = 1 if f["time_bin"]=="midday" else 0
    f["bin_afternoon"] = 1 if f["time_bin"]=="afternoon" else 0
    f["bin_evening"] = 1 if f["time_bin"]=="evening" else 0
    f["bin_night"] = 1 if f["time_bin"]=="night" else 0

    trainning_results = joblib.load(os.path.join(MODEL_DIR, "session_models.pkl"))
    scaler = trainning_results["scaler"]
    kmeans = trainning_results["kmeans"]
    dbscan = trainning_results["dbscan"]
    iso = trainning_results["isolation"]
    cols = trainning_results["columns"]

    row_vector = np.array([[f.get(c, 0) for c in cols]])
    X = scaler.transform(row_vector)

    km = int(kmeans.predict(X)[0])
    db = int(dbscan.fit_predict(X)[0])  # -1 = noise
    anom = int(iso.predict(X)[0])       # -1 = anomaly

    return {
        "cluster_kmeans": km,
        "cluster_dbscan": db,
        "anomaly_iforest": anom
    }



@app.route("/train", methods=["POST"])
def train_endpoint():
    payload = request.get_json()
    if not payload or "ev_sessions" not in payload:
        return jsonify({"error":"expected field 'ev_sessions'"}), 400
    try:
        meta = train_models(payload["ev_sessions"])
        return jsonify({"status": "ok", "meta": meta})
    except Exception as e:
        print("ERROR TRAIN:", e)
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/predict_session", methods=["GET"])
def predict_endpoint():
    payload = request.get_json()
    if not payload or "ev_session" not in payload:
        return jsonify({"error":"expected field 'session'"}), 400
    try:
        res = predict_session(payload["ev_session"])
        return jsonify({"status":"ok","result":res})
    except Exception as e:
        print("ERROR PREDICT:", e, flush=True)
        return jsonify({"status":"error","error":str(e)}), 500


if __name__ == "__main__":
    print("Starting ML server!")
    serve(app, host="0.0.0.0", port=5000)
