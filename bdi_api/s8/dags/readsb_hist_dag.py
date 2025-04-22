import gzip
import io
import json
import logging
import os
from datetime import datetime, timedelta

import boto3
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv

load_dotenv()

# Set up
BUCKET_NAME = 'bdi-aircraft-andreabarboza'
RAW_PATH = 'raw/day=20231101/'
PREPARED_PATH = 'prepared/day=20231101/'
BASE_URL = "https://samples.adsbexchange.com/readsb-hist/"
s3 = boto3.client('s3')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_file_from_s3(file_key):
    """Fetch and parse a single file from S3."""
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
    content = obj["Body"].read()
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz:
            data = json.loads(gz.read().decode("utf-8"))
    except:
        data = json.loads(content.decode("utf-8"))
    aircraft_data = data.get("aircraft", data) if isinstance(data, dict) else data
    for record in aircraft_data:
        logger.info(f"Processing record: {record}")
    return aircraft_data

def download_readsb_hist_data(day: str, **kwargs):
    """Fetch multiple existing files from S3 raw layer."""
    aircraft_data_all = []
    num_files_to_fetch = 10
    for i in range(num_files_to_fetch):
        minute = f"{i:06d}"  # e.g., 000000Z, 000001Z, etc.
        file_key = f"{RAW_PATH}{minute}Z.json.gz"
        try:
            aircraft_data = get_file_from_s3(file_key)
            aircraft_data_all.extend(aircraft_data)
            logger.info(f"Successfully fetched {file_key}")
        except Exception as e:
            logger.error(f"Error fetching {file_key}: {e}")
            continue  # Skip missing or errored files
    return aircraft_data_all

def prepare_readsb_hist_data(day: str, **kwargs):
    """Transform fetched data and store in S3 prepared layer."""
    ti = kwargs['ti']
    aircraft_data_all = ti.xcom_pull(task_ids='download_readsb_hist_data')

    # Basic transformation (e.g., filter or clean data if needed)
    prepared_data = [
        {
            "icao": record.get("hex"),
            "icaotype": record.get("type"),
            "registration": record.get("r"),
            "timestamp": day
        } for record in aircraft_data_all if record.get("hex")
    ]

    # Save to S3 prepared layer
    prepared_key = f"{PREPARED_PATH}aircraft_prepared.json"
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=prepared_key,
        Body=json.dumps(prepared_data).encode("utf-8")
    )

#This function will upload prepared JSON data from S3 and upload into PostgreSQL
def upload_to_postgresql(day: str, **kwargs):
    """Upload prepared data to PostgreSQL with idempotency."""
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=5432,
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    cursor = conn.cursor()

    prepared_key = f"{PREPARED_PATH}aircraft_prepared.json"
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=prepared_key)
    prepared_data = json.loads(obj["Body"].read().decode("utf-8"))

    # Insert with idempotency
    for record in prepared_data:
        cursor.execute("""
            INSERT INTO aircraft_sightings (icao, icaotype, registration, timestamp)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (icao, timestamp) DO NOTHING
        """, (record["icao"], record["icaotype"], record["registration"], day))

    conn.commit()
    cursor.close()
    conn.close()

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'readsb_hist_dag',
    default_args=default_args,
    description='Process existing readsb-hist data from S3',
    schedule_interval='@monthly',  # Runs on 1st of each month
    start_date=datetime(2023, 11, 1),
    end_date=datetime(2024, 11, 1),
    catchup=True,
) as dag:
    download_task = PythonOperator(
        task_id='download_readsb_hist_data',
        python_callable=download_readsb_hist_data,
        op_args=['{{ ds }}'],
        provide_context=True,
    )
    prepare_task = PythonOperator(
        task_id='prepare_readsb_hist_data',
        python_callable=prepare_readsb_hist_data,
        op_args=['{{ ds }}'],
        provide_context=True,
    )
    upload_task = PythonOperator(
        task_id='upload_to_postgresql',
        python_callable=upload_to_postgresql,
        op_args=['{{ ds }}'],
    )

    download_task >> prepare_task >> upload_task
