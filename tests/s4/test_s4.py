# tests/s4/test_s4.py
import json
import os
from unittest import mock

import boto3
import pytest
from moto import mock_s3
from fastapi.testclient import TestClient
from bdi_api import app
from bdi_api.s4.exercise import s4
from bdi_api.settings import Settings

client = TestClient(app)
settings = Settings()

@pytest.fixture
def s3_bucket():
    """Create a mock S3 bucket for testing."""
    with mock_s3():
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket_name = settings.s3_bucket
        s3.create_bucket(Bucket=bucket_name)
        yield bucket_name

def test_download_data_success(s3_bucket):
    """Test successful file download and upload to S3."""
    with mock.patch("requests.get") as mock_get:  # Mock requests.get directly
        mock_list_response = mock.Mock()
        mock_list_response.status_code = 200
        mock_list_response.text = """<a href='file1.json.gz'>file1.json.gz</a>"""
        
        mock_file_response = mock.Mock()
        mock_file_response.status_code = 200
        mock_file_response.content = b"test content"
        mock_file_response.raise_for_status = mock.Mock()

        mock_get.side_effect = [mock_list_response, mock_file_response]
        
        response = client.post("/api/s4/aircraft/download", params={"file_limit": 1})
        
        assert response.status_code == 200
        assert "Downloaded 1 files in S3" in response.text

def test_download_data_failure(s3_bucket):
    """Test failed file download."""
    with mock.patch("requests.get", side_effect=Exception("Connection error")):  # Mock requests.get directly
        response = client.post("/api/s4/aircraft/download", params={"file_limit": 1})
        assert response.status_code == 200
        assert "Error accessing URL" in response.text

def test_prepare_data_success(s3_bucket):
    """Test successful data preparation from S3."""
    test_dir = settings.prepared_dir  # Use the property
    os.makedirs(test_dir, exist_ok=True)

    test_data = {
        "now": 1635724800,
        "aircraft": [{
            "hex": "abc123",
            "r": "N123AB",
            "t": "B738",
            "lat": 40.7128,
            "lon": -74.0060,
            "alt_baro": 30000,
            "gs": 450,
            "alert": 0
        }]
    }
    
    s3 = boto3.client("s3", region_name="us-east-1")
    test_key = "raw/day=20231101/test.json.gz"
    s3.put_object(
        Bucket=s3_bucket,
        Key=test_key,
        Body=json.dumps(test_data).encode()
    )
    
    response = client.post("/api/s4/aircraft/prepare")
    
    assert response.status_code == 200
    assert "Prepared data saved" in response.text
    
    processed_file = os.path.join(test_dir, "test.json")
    assert os.path.exists(processed_file)

    # Cleanup
    os.remove(processed_file)
    if not os.listdir(test_dir):  # Only remove if directory is empty
        os.rmdir(test_dir)

def test_prepare_data_failure(s3_bucket):
    """Test preparation when no files exist in S3."""
    response = client.post("/api/s4/aircraft/prepare")
    
    assert response.status_code == 200
    assert "No files found in S3" in response.text