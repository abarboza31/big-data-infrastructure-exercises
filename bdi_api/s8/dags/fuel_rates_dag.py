from datetime import datetime

import boto3
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

BUCKET_NAME = 'bdi-aircraft-andreabarboza'
s3 = boto3.client('s3')

def download_fuel_rates():
    url = "https://raw.githubusercontent.com/martsec/flight_co2_analysis/main/data/aircraft_type_fuel_consumption_rates.json"
    response = requests.get(url)
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key='static/aircraft_type_fuel_consumption_rates.json',
        Body=response.content
    )

with DAG(
    'fuel_rates_dag',
    default_args={'owner': 'airflow'},
    description='Download fuel consumption rates',
    schedule_interval='@once',
    start_date=datetime(2025, 4, 1),
    catchup=False,
) as dag:
    download_task = PythonOperator(
        task_id='download_fuel_rates',
        python_callable=download_fuel_rates,
    )
