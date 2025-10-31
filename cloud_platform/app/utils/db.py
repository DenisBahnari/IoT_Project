import psycopg2 # type: ignore
import json
import time
from psycopg2.extras import RealDictCursor # type: ignore
from datetime import datetime

def get_db_connection(max_retries=10, wait_seconds=1):
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(
                host="db",
                database="mydatabase",
                user="user",
                password="password"
            )
            return conn

        except psycopg2.OperationalError as e:
            print(f"Connection {attempt}/{max_retries} failed: {e}")
            if attempt == max_retries:
                raise e
            time.sleep(wait_seconds)


def insert_ev_data(json_data):
    try:
        row = json.loads(json_data)
    except json.JSONDecodeError:
        print("Invalid JSON data insert")
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO ev_session (
            user_id, vehicle_model, battery_capacity_kwh, station_id,
            start_time, end_time, energy_consumed_kwh, duration_h,
            charging_rate_kw, charging_cost_eur, time_of_day, day_of_week,
            soc_start, soc_end, distance_driven_km, temperature_c, vehicle_age_years
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ON CONSTRAINT unique_session DO NOTHING;
    """

    cursor.execute(insert_query, (
        row.get("\ufeffUser ID"),
        row.get("Vehicle Model"),
        _to_int(row.get("Battery Capacity (kWh)")),
        row.get("Charging Station ID"),
        _to_timestamp(row.get("Charging Start Time")),
        _to_timestamp(row.get("Charging End Time")),
        _to_float(row.get("Energy Consumed (kWh)")),
        _to_float(row.get("Charging Duration (hours)")),
        _to_float(row.get("Charging Rate (kW)")),
        _to_float(row.get("Charging Cost (EUR)")),
        row.get("Time of Day"),
        row.get("Day of Week"),
        _to_float(row.get("State of Charge (Start %)")),
        _to_float(row.get("State of Charge (End %)")),
        _to_float(row.get("Distance Driven (since last charge) (km)")),
        _to_float(row.get("Temperature (C)")),
        _to_int(row.get("Vehicle Age (years)"))
    ))

    conn.commit()
    cursor.close()
    conn.close()

def insert_station_data(json_data):
    try:
        row = json.loads(json_data)
    except json.JSONDecodeError:
        print("Invalid JSON data insert")
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO ev_station (
            station_id, distrito, concelho, freguesia,
            latitude, longitude, potencia_max_kw, num_pontos_ligacao,
            cod_distrito, cod_distrito_concelho, cod_distrito_concelho_freguesia
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (station_id) DO NOTHING;
    """

    cursor.execute(insert_query, (
        row.get("\ufeffStation ID"),
        row.get("Distrito"),
        row.get("Concelho"),
        row.get("Freguesia"),
        _to_float(row.get("Latitude")),
        _to_float(row.get("Longitude")),
        _to_float(row.get("Potência Máxima Admissível (kW)")),
        _to_int(row.get("Pontos de ligação para instalações de PCVE")),
        _to_int(row.get("CodDistrito")),
        _to_int(row.get("CodDistritoConcelho")),
        _to_int(row.get("CodDistritoConcelhoFreguesia"))
    ))

    conn.commit()
    cursor.close()
    conn.close()

def _to_float(value):
    if value in (None, "", " "):
        return None
    try:
        return float(str(value).replace(",", ".").strip())
    except ValueError:
        return None

def _to_int(value):
    if value in (None, "", " "):
        return None
    try:
        return int(float(str(value).replace(",", ".").strip()))
    except ValueError:
        return None

def _to_timestamp(value):
    if not value or value.strip() == "":
        return None
    try:
        return datetime.strptime(value.strip(), "%d/%m/%y %H:%M")
    except ValueError:
        return None
    


def get_ev_session_by_id(id):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        insert_query = """
            SELECT * 
            FROM ev_session 
            WHERE id = %s;
        """

        cursor.execute(insert_query, (_to_int(id),))
        row = cursor.fetchone()

        if row:
            return row
        else:
            print("EV session with %s not found." % id)

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error getting from DB: {e}")
