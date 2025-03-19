import json
import os
from typing import Annotated
from urllib.parse import urljoin

import boto3
import requests
from bs4 import BeautifulSoup
from fastapi import APIRouter, status, HTTPException
from fastapi.params import Query
from tqdm import tqdm

from bdi_api.settings import Settings

settings = Settings()
s3 = boto3.client("s3")

s4 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s4",
    tags=["s4"],
)


@s4.post("/aircraft/download")
def download_data(
    file_limit: Annotated[int, Query(..., description="Limits the number of files to download.")],
) -> str:
    """Download files from a source URL and store them in S3."""
    base_url = settings.source_url + "/2023/11/01/"
    s3_bucket = settings.s3_bucket
    s3_prefix_path = "raw/day=20231101/"

    try:
        try:
            response = requests.get(base_url)
            response.raise_for_status()
        except Exception as e:
            return f"Error accessing URL: {str(e)}"

        soup = BeautifulSoup(response.text, "html.parser")
        files = [a["href"] for a in soup.find_all("a") if a["href"].endswith(".json.gz")][:file_limit]

        downloaded_count = 0
        for file_name in tqdm(files, desc="Downloading files"):
            file_url = urljoin(base_url, file_name)
            try:
                response = requests.get(file_url, stream=True)
                response.raise_for_status()
            except Exception as e:
                return f"Error accessing URL: {str(e)}"

            s3_key = f"{s3_prefix_path}{file_name}"
            s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=response.content, ContentType="application/json")
            downloaded_count += 1

        return f"Downloaded {downloaded_count} files in S3."

    except Exception as e:
        raise HTTPException(status_code=200, detail=f"Error accessing URL: {str(e)}")


@s4.post("/aircraft/prepare")
def prepare_data() -> str:
    """Download data from S3 and store it in the local `prepared` directory."""
    s3_bucket = settings.s3_bucket
    s3_prefix_path = "raw/day=20231101/"
    local_directory = settings.prepared_dir  # Use the property

    try:
        os.makedirs(local_directory, exist_ok=True)

        # Clear old data in prepared directory
        for file in os.listdir(local_directory):
            os.remove(os.path.join(local_directory, file))

        response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix_path)
        if 'Contents' not in response:
            return "No files found in S3."

        for obj in tqdm(response['Contents'], desc="Processing files"):
            s3_key = obj['Key']
            file_name = os.path.basename(s3_key)
            prepared_file_path = os.path.join(local_directory, file_name.replace(".gz", ""))

            try:
                file_response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
                json_content = json.loads(file_response['Body'].read().decode('utf-8'))

                timestamp = json_content.get("now")
                aircraft_data = json_content.get("aircraft", [])

                processed_aircraft_data = [
                    {
                        "icao": record.get("hex"),
                        "registration": record.get("r"),
                        "type": record.get("t"),
                        "lat": record.get("lat"),
                        "lon": record.get("lon"),
                        "alt_baro": record.get("alt_baro"),
                        "timestamp": timestamp,
                        "max_altitude_baro": record.get("alt_baro"),
                        "max_ground_speed": record.get("gs"),
                        "had_emergency": record.get("alert", 0) == 1,
                    }
                    for record in aircraft_data
                ]

                with open(prepared_file_path, "w", encoding="utf-8") as f:
                    json.dump(processed_aircraft_data, f)

            except Exception as e:
                print(f"Error processing file {s3_key}: {str(e)}")
                continue

        return f"Prepared data saved to {local_directory}."

    except Exception as e:
        return f"Error during preparation: {str(e)}"