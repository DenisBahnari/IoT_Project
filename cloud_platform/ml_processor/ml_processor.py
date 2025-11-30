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

SESSION_CLUSTERS = 4         
DBSCAN_EPS = 0.5
DBSCAN_MIN_SAMPLES = 10
IFOREST_CONTAM = 0.0001      

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
    soc_s = safe_float(r.get("State of Charge (Start %)") )
    soc_e = safe_float(r.get("State of Charge (End %)") )
    dist = safe_float(r.get("Distance Driven (since last charge) (km)") )
    temp = safe_float(r.get("Temperature (C)") )
    age  = safe_float(r.get("Vehicle Age (years)") )

    soc_delta = np.nan
    if not np.isnan(soc_s) and not np.isnan(soc_e):
        soc_delta = soc_e - soc_s

    energy_rel = np.nan
    if battery_kwh and battery_kwh > 0 and not np.isnan(energy):
        energy_rel = energy / battery_kwh

    hour = start.hour if start is not None else np.nan

    intensity = np.nan
    if duration_h and duration_h > 0 and not np.isnan(energy):
        intensity = energy / duration_h

    if np.isnan(dist):
        dist = 0.0

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
        "energy_rel": energy_rel,
        "intensity": intensity
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
    df = pd.DataFrame(feat_rows).fillna(0)

    scaler = StandardScaler()
    X = scaler.fit_transform(df.values)

    kmeans = KMeans(n_clusters=SESSION_CLUSTERS, random_state=42, n_init=20)
    kmeans.fit(X)

    dbscan = DBSCAN(eps=DBSCAN_EPS, min_samples=DBSCAN_MIN_SAMPLES)
    dbscan.fit(X)

    iso = IsolationForest(contamination=IFOREST_CONTAM, random_state=42)
    iso.fit(X)

    joblib.dump({
        "scaler": scaler,
        "kmeans": kmeans,
        "dbscan": dbscan,
        "isolation": iso,
        "columns": SESSION_NUM_COLS
    }, os.path.join(MODEL_DIR, "session_models.pkl"))

    meta = {
        "n_sessions": len(df),
        "n_features": len(SESSION_NUM_COLS),
        "kmeans_clusters": SESSION_CLUSTERS
    }

    with open(os.path.join(MODEL_DIR, "meta.json"), "w") as f:
        json.dump(meta, f)

    return meta

def predict_session(raw):
    d = row_from_raw(raw)
    f = featurize_session(d)

    trainning_results = joblib.load(os.path.join(MODEL_DIR, "session_models.pkl"))
    scaler = trainning_results["scaler"]
    kmeans = trainning_results["kmeans"]
    dbscan = trainning_results["dbscan"]
    iso = trainning_results["isolation"]
    cols = trainning_results["columns"]

    for col in cols:
        if col not in f:
            f[col] = 0

    row_vector = np.array([[f.get(c, 0) for c in cols]])
    X = scaler.transform(row_vector)

    km = int(kmeans.predict(X)[0])
    db = int(dbscan.fit_predict(X)[0])

    return {
        "cluster_kmeans": km,
        "cluster_dbscan": db
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

@app.route("/predict_all_sessions", methods=["GET"])
def predict_all_endpoint():
    payload = request.get_json()
    if not payload or "ev_sessions" not in payload:
        return jsonify({"error":"expected field 'ev_sessions'"}), 400
    try:
        results = {}
        for raw in payload["ev_sessions"]:
            res = predict_session(raw)
            results[raw[0]] = res
        return jsonify({"status":"ok","results":results})
    except Exception as e:
        print("ERROR PREDICT ALL:", e, flush=True)
        return jsonify({"status":"error","error":str(e)}), 500

if __name__ == "__main__":
    print("Starting ML server!")
    serve(app, host="0.0.0.0", port=5000)
