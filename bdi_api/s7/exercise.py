import gzip
import io
import json

import boto3
import psycopg2
from fastapi import APIRouter
from psycopg2.extras import execute_batch

from bdi_api.settings import DBCredentials, Settings

#Initialize the settings and db_credentials
settings = Settings()
db_credentials = DBCredentials()

# Initialize AWS S3 client
s3_client = boto3.client("s3")
#Router configuration
s7 = APIRouter(prefix="/api/s7", tags=["s7"])

#Include the read s3 bucket code and then postgres connection code

# S3 bucket details
BUCKET_NAME = settings.s3_bucket
OBJECT_KEY = "raw/day=20231101/000015Z.json.gz"

#Connection to database
#pool the connection
def connect_to_db():
    conn_params = {
        "dbname": db_credentials.username,
        "user": db_credentials.username,
        "password": db_credentials.password,
        "host": db_credentials.host,
        "port": db_credentials.port,
    }
    return psycopg2.connect(**conn_params)

#Create tables if database is empty
def create_tables():
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute(
            """
            CREATE TABLE IF NOT EXISTS aircraft (
                icao VARCHAR PRIMARY KEY,
                registration VARCHAR,
                type VARCHAR
            );
            CREATE TABLE IF NOT EXISTS aircraft_positions (
                icao VARCHAR REFERENCES aircraft(icao),
                timestamp BIGINT,
                lat DOUBLE PRECISION,
                lon DOUBLE PRECISION,
                altitude_baro DOUBLE PRECISION,
                ground_speed DOUBLE PRECISION,
                emergency BOOLEAN,
                PRIMARY KEY (icao, timestamp)
            );
            """
        )
    conn.commit()
    cur.close()
    conn.close()

def fetch_files_from_s3():
    # Lists all the files in the bucketand processes each one
    all_data = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET_NAME):
        for obj in page.get("Contents", []):
            file_key = obj["Key"]
            file_data = get_file_from_s3(file_key)
            all_data.extend(file_data)
    return all_data

#Fetches and parses a single gzip file from S3
def get_file_from_s3(file_key):
    obj = s3_client.get_object(Bucket=settings.s3_bucket, Key=file_key)
    content = obj["Body"].read()

# Try-except handles gzipped files in the event they aren't actually compressed
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz:
            data = json.loads(gz.read().decode("utf-8"))
    except Exception:
        data = json.loads(content.decode("utf-8"))

    return data.get("aircraft", data) if isinstance(data, dict) else data

#Save to database
def save_to_db(data):
    conn = connect_to_db()
    cur = conn.cursor()

    aircraft_data = []
    position_data = []

    # Handle the case where data is a list of aircraft (as in the current JSON structure)
    data.get("aircraft", data) if isinstance(data, dict) else data

    for record in data:
        if not isinstance(record, dict):
            continue

        icao = record.get("hex") or record.get("icao")
        if not icao:
            continue

        # Use "r" as the registration field, with fallback to None
        registration = record.get("r")  # Directly use "r" as the primary key
        aircraft_type = record.get("type", "unknown")

        print(f"Processing record: {record}")  # Debug output

        aircraft_data.append((
            icao,
            registration,
            aircraft_type
        ))

        if "lat" in record and "lon" in record:
            # Handle 'ground' value for alt_baro
            alt_baro_value = record.get("alt_baro", 0)
            alt_baro = 0. if str(alt_baro_value).lower() == 'ground' else float(alt_baro_value)

            # Use baro_rate as ground_speed placeholder to match the table schema
            ground_speed_value = record.get("baro_rate", 0)
            ground_speed = 0. if str(ground_speed_value).lower() == 'ground' else float(ground_speed_value)

            position_data.append((
                icao,
                record.get("timestamp", 0),
                record["lat"],
                record["lon"],
                alt_baro,
                ground_speed,
                bool(record.get("emergency", False))
            ))

    if aircraft_data:
        execute_batch(cur, """
            INSERT INTO aircraft (icao, registration, type)
            VALUES (%s, %s, %s)
            ON CONFLICT (icao) DO UPDATE SET
                registration = EXCLUDED.registration,
                type = EXCLUDED.type
        """, aircraft_data)

    if position_data:
        execute_batch(cur, """
            INSERT INTO aircraft_positions
            (icao, timestamp, lat, lon, altitude_baro, ground_speed, emergency)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (icao, timestamp) DO NOTHING
        """, position_data)

    conn.commit()
    cur.close()
    conn.close()

@s7.post("/aircraft/prepare")
def prepare_data() -> str:
    # TODO
    file_key = "raw/day=20231101/000015Z.json.gz"
    create_tables()
    data = get_file_from_s3(file_key)
    if not data:
        return "No aircraft data found"
    save_to_db(data)
    return "Data successfully inserted into RDS"
    """Get the raw data from s3 and insert it into RDS

    Use credentials passed from `db_credentials`
    """

@s7.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc FROM THE DATABASE

    Use credentials passed from `db_credentials`
    """
    # TODO
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT icao, registration, type FROM aircraft ORDER BY icao LIMIT %s OFFSET %s",
        (num_results, page * num_results)
    )
    results = [{"icao": r[0], "registration": r[1], "type": r[2]} for r in cur.fetchall()]
    cur.close()
    conn.close()
    return results


@s7.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list. FROM THE DATABASE

    Use credentials passed from `db_credentials`
    """
    # TODO
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT timestamp, lat, lon FROM aircraft_positions WHERE icao = %s ORDER BY timestamp LIMIT %s OFFSET %s",
        (icao, num_results, page * num_results)
    )
    results = cur.fetchall()
    print(f"Query results for icao={icao}: {results}")  # Debug output
    response = [{"timestamp": r[0], "lat": r[1], "lon": r[2]} for r in results]
    cur.close()
    conn.close()
    return response



@s7.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed
    * had_emergency

    FROM THE DATABASE

    Use credentials passed from `db_credentials`
    """
    # TODO
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COALESCE(MAX(altitude_baro), 0),
               COALESCE(MAX(ground_speed), 0),
               COALESCE(BOOL_OR(emergency), FALSE)
        FROM aircraft_positions WHERE icao = %s
        """,
        (icao,)
    )
    row = cur.fetchone()
    result = {
        "max_altitude_baro": row[0],
        "max_ground_speed": row[1],
        "had_emergency": row[2]
    }
    cur.close()
    conn.close()
    return result

if __name__ == "__main__":
    print(prepare_data())
