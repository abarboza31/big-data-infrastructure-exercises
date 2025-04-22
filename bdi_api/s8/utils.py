# s8/utils.py
import gzip
import io
import json
import os

import boto3
from settings import Settings
from tqdm import tqdm

settings = Settings()
s3_client = boto3.client('s3')
RAW_DIR = os.path.join("data", "raw")
PREPARED_DIR = os.path.join("data", "prepared")
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PREPARED_DIR, exist_ok=True)

def get_file_from_s3(file_key):
    obj = s3_client.get_object(Bucket=settings.s3_bucket, Key=file_key)
    content = obj["Body"].read()
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz:
            data = json.loads(gz.read().decode("utf-8"))
    except Exception:
        data = json.loads(content.decode("utf-8"))
    return data.get("aircraft", data) if isinstance(data, dict) else data

def fetch_files_from_s3():
    try:
        for file in os.listdir(RAW_DIR):
            os.remove(os.path.join(RAW_DIR, file))
        paginator = s3_client.get_paginator("list_objects_v2")
        file_count = 0
        max_files = 100
        for page in paginator.paginate(Bucket=settings.s3_bucket, Prefix="raw/day=20231101/"):
            for obj in page.get("Contents", []):
                if file_count >= max_files:
                    break
                file_key = obj["Key"]
                file_name = os.path.basename(file_key)
                raw_file_path = os.path.join(RAW_DIR, file_name)
                file_data = get_file_from_s3(file_key)
                if file_data is None:
                    continue
                with open(raw_file_path, 'w') as f:
                    json.dump(file_data, f)
                file_count += 1
                print(f"Downloaded file {file_count}/{max_files}: {file_name}")
            if file_count >= max_files:
                break
        print(f"Successfully downloaded {file_count} files to {RAW_DIR}")
        return True
    except Exception as e:
        print(f"Error fetching files from S3: {str(e)}")
        return False

def process_raw_to_prepared():
    try:
        for file in os.listdir(PREPARED_DIR):
            os.remove(os.path.join(PREPARED_DIR, file))
        aggregated_data = {}
        for file_name in tqdm(os.listdir(RAW_DIR), desc="Processing raw files"):
            raw_file_path = os.path.join(RAW_DIR, file_name)
            with open(raw_file_path) as f:
                raw_data = json.load(f)
            for aircraft in raw_data:
                icao = aircraft.get("icao")
                if not icao:
                    continue
                cleaned_entry = {
                    "ownop": aircraft.get("ownop", None),
                    "manufacturer": aircraft.get("manufacturer", None),
                    "model": aircraft.get("model", None)
                }
                cleaned_entry = {k: v for k, v in cleaned_entry.items() if v is not None}
                if cleaned_entry:
                    aggregated_data[icao] = cleaned_entry
        prepared_file_path = os.path.join(PREPARED_DIR, "aircraft_db.json")
        with open(prepared_file_path, 'w') as f:
            json.dump(aggregated_data, f)
        print(f"Successfully processed data and saved to {prepared_file_path}")
        return True
    except Exception as e:
        print(f"Error processing raw to prepared: {str(e)}")
        return False
