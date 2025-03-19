
from bdi_api.app import app as real_app
import io
import json
import gzip
import os
from unittest import mock
import pytest
from moto import mock_s3
import boto3
from fastapi.testclient import TestClient
from bdi_api.settings import Settings
from bdi_api.s7.exercise import prepare_data, s7

settings = Settings()
client = TestClient(s7)

@pytest.fixture
def s3_bucket():
    """Create a mock S3 bucket for testing."""
    with mock_s3():
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket_name = "bdi-aircraft-andreabarboza"
        s3.create_bucket(Bucket=bucket_name)
        yield bucket_name

def test_prepare_data_success(s3_bucket):
    """Test successful data retrieval and insertion from S3."""
    with mock.patch("bdi_api.s7.exercise.connect_to_db", return_value=mock.MagicMock()):
        test_data = {
            "aircraft": [
                {
                    "hex": "abc123",
                    "r": "N123AB",
                    "type": "B738",
                    "lat": 40.7128,
                    "lon": -74.0060,
                    "alt_baro": 30000,
                }
            ]
        }

    compressed_data = io.BytesIO()
    with gzip.GzipFile(fileobj=compressed_data, mode="w") as gz:
        gz.write(json.dumps(test_data).encode("utf-8"))
    compressed_data.seek(0)

    s3 = boto3.client("s3", region_name="us-east-1")
    test_key = "raw/day=20231101/test.json.gz"
    s3.put_object(Bucket=s3_bucket, Key=test_key, Body=compressed_data.read())

    with mock.patch("bdi_api.s7.exercise.fetch_files_from_s3", return_value=test_data["aircraft"]):
        with mock.patch("bdi_api.s7.BUCKET_NAME", s3_bucket):
            response = client.post("/api/s7/aircraft/prepare")

    assert response.status_code == 200
    assert "Data successfully inserted into RDS" in response.text

@mock.patch("bdi_api.s7.exercise.get_file_from_s3", return_value={})
def test_prepare_data_failure(mock_get_file_from_s3, s3_bucket):
    """Test handling of missing aircraft data."""
    response = client.post("/api/s7/aircraft/prepare")
    assert response.status_code == 200
    assert "No aircraft data found" in response.text

def test_list_aircraft():
    """Test retrieving the list of aircraft from the database."""
    response = client.get("/api/s7/aircraft/", params={"num_results": 10, "page": 0})
    assert response.status_code == 200
    assert isinstance(response.json(), list)

def test_get_aircraft_positions():
    """Test retrieving aircraft position history from the database."""
    response = client.get("/api/s7/aircraft/abc123/positions", params={"num_results": 10, "page": 0})
    assert response.status_code == 200
    assert isinstance(response.json(), list)

def test_get_aircraft_statistics():
    """Test retrieving aircraft statistics from the database."""
    response = client.get("/api/s7/aircraft/abc123/stats")
    assert response.status_code == 200
    assert isinstance(response.json(), dict)
    assert "max_altitude_baro" in response.json()
    assert "max_ground_speed" in response.json()
    assert "had_emergency" in response.json()