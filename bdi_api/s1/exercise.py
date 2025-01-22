import os
import requests
import pandas as pd
from typing import List
from urllib.parse import urljoin
import json
from tqdm import tqdm
from typing import Annotated
from bs4 import BeautifulSoup

from fastapi import APIRouter, status
from fastapi.params import Query
from fastapi.responses import FileResponse
from fastapi import HTTPException
from bdi_api.settings import Settings

settings = Settings()

s1 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s1", #download postman
    tags=["s1"],
)
#test 1


@s1.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int,
        Query(
            ...,
            description="""
    Limits the number of files to download.
    You must always start from the first the page returns and
    go in ascending order in order to correctly obtain the results.
    I'll test with increasing number of files starting from 100.""",
        ),
    ] = 100,
) -> str:
    """Downloads the `file_limit` files AS IS inside the folder data/20231101

    data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to configure it in the `pyproject.toml` file
    so it can be installed using `poetry update`.


    TIP: always clean the download folder before writing again to avoid having old files.
    """
    download_dir = os.path.join(settings.raw_dir, "day=20231101")
    base_url = settings.source_url + "/2023/11/01/"
    # TODO Implement download

    # Step 1: I will check to ensure the download directory exists and create it if it doesn't
    os.makedirs(download_dir, exist_ok=True)

    # Clean the download directory for existing files
    for file in os.listdir(download_dir):
        file_path = os.path.join(download_dir, file)
        if os.path.isfile(file_path): 
            os.remove(file_path)


    try:
        # Get list of files from the Swagger UI link
        response = requests.get(base_url)
        # Added error handling for HTTP status
        response.raise_for_status()  
        
        # Parse the HTML content with BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        files = [
            a['href'] for a in soup.find_all('a') 
            if a['href'].endswith('.json.gz')
        ][:file_limit] # Apply the file limit of 10 
        
        # A count of successfully downloaded files
        downloaded_count = 0
        for file_name in tqdm(files, desc="Downloading files"):
            file_url = urljoin(base_url, file_name)
            response = requests.get(file_url, stream=True)
            
            if response.status_code == 200:
                file_path = os.path.join(download_dir, file_name[:-3])
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                downloaded_count += 1
            else:
                print(f"Failed download {file_name}")

        
        return f"Downloaded {downloaded_count} files to {download_dir}"
    
    except requests.RequestException as e:
        return f"Error accessing URL: {str(e)}"
    except Exception as e:
        return f"Error during download: {str(e)}"


    return "OK"


@s1.post("/aircraft/prepare")
def prepare_data() -> str:
    """Prepare the data in the way you think it's better for the analysis.

    * data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    * documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to configure it in the `pyproject.toml` file
    so it can be installed using `poetry update`.

    TIP: always clean the prepared folder before writing again to avoid having old files.

    Keep in mind that we are downloading a lot of small files, and some libraries might not work well with this!
    """
    # TODO
    prepared_directory = os.path.join(settings.prepared_dir, "day=20231101")
    raw_data_dir = os.path.join(settings.raw_dir, "day=20231101")

    # Create directory if it doesn't exist
    os.makedirs(prepared_directory, exist_ok=True)

    # Clean existing files
    for file in os.listdir(prepared_directory):
        file_path = os.path.join(prepared_directory, file)
        if os.path.isfile(file_path):
            os.remove(file_path)

    # The code reads JSON files from the raw data directory and processes them into Dataframes
    try:
        aircraft_data = []
        for file in os.listdir(raw_data_dir):
            if file.endswith('.json'):
                file_path = os.path.join(raw_data_dir, file)
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    if "aircraft" in data:
                        df = pd.DataFrame(data['aircraft'])
                        # Add timestamp directly to DataFrame
                        df['timestamp'] = data['now']
                        aircraft_data.append(df)
        
        if not aircraft_data:
            return "No aircraft data found."

        # All DataFrames are concatenated into a single Dataframe
        processed_data = pd.concat(aircraft_data, ignore_index=True)

        # Select required columns
        processed_data = processed_data[['hex', 'r', 'type', 't', 'lat', 'lon', 'alt_baro', 'gs', 'emergency', 'timestamp']]

        # Rename columns
        processed_data = processed_data.rename(columns={
            'hex': 'icao',
            'r': 'registration',
            't': 'type',
            'alt_baro': 'altitude_baro',
            'gs': 'ground_speed',
            'emergency': 'had_emergency'
        })

        # Drop rows with NaN values in 'icao', 'registration', and 'type'
        processed_data = processed_data.dropna(subset=['icao', 'registration', 'type'])

        # Process emergency flags
        emergency_flags = {'general', 'lifeguard', 'minfuel', 'nordo', 'unlawful', 'downed', 'reserved'}
        processed_data['had_emergency'] = processed_data['had_emergency'].apply(lambda x: x in emergency_flags)

        # Remove rows with any NaN values
        processed_data = processed_data.dropna()

        # Save as CSV
        output_file = os.path.join(prepared_directory, 'prepared_data.csv')
        processed_data.to_csv(output_file, index=False)

        print(processed_data)
        return f"Data prepared and saved to {output_file}"

    except Exception as e:
        return f"An error has occurred during data preparation: {str(e)}"
    return "OK"


@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc
    """
    # TODO
    # This variable directly constructs the full path to the specific and only file with all the prepared data
    prepared_directory = os.path.join(settings.prepared_dir, "day=20231101", "prepared_data.csv")
    if not os.path.exists(prepared_directory):
        return []

    if not os.path.exists(prepared_directory):
        return []

    sorted_aircraft = pd.read_csv(prepared_directory)
    # Deduplicate values and sort by 'icao'
    sorted_aircraft = sorted_aircraft[['icao', 'registration', 'type']].drop_duplicates().sort_values(by='icao')

    start = page * num_results
    end = start + num_results

    result = sorted_aircraft.iloc[start:end].to_dict(orient='records')
    print(result)
    return result

    return [{"icao": "0d8300", "registration": "YV3382", "type": "LJ31"}] # Done


@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list.
    """
    # TODO implement and return a list with dictionaries with those values.

    # Calculate the start index: start_index = page * limit
    start_index = page * num_results
    # Calculate the end index: end_index = (page + 1) * limit
    end_index = (page + 1) * num_results
    prepared_directory = os.path.join(settings.prepared_dir, "day=20231101", "prepared_data.csv")
    if not os.path.exists(prepared_directory):
        return []

    df = pd.read_csv(prepared_directory)
    # Dataframe with subset of rows for the requested 'icao'
    filtered_df = df[df["icao"] == icao].sort_values(by="timestamp")
    # Slice the DataFrame according to the page and number of results per page
    return filtered_df.iloc[page * num_results:(page + 1) * num_results][["timestamp", "lat", "lon"]].to_dict(orient="records")

    return [{"timestamp": 1609275898.6, "lat": 30.404617, "lon": -86.476566}]


@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed
    * had_emergency
    """
    # TODO Gather and return the correct statistics for the requested aircraft
    data_file = os.path.join(settings.prepared_dir, "day=20231101", "prepared_data.csv")
    
    # Check if the file exists
    if not os.path.exists(data_file):
        return {}

    try:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(data_file)
        
        # Filter the DataFrame by 'icao'
        df = df[df['icao'] == icao]

        # Check if the filtered DataFrame is empty
        if df.empty:
            return {}

        # Remove rows with NaN values in relevant columns
        df = df.dropna(subset=['altitude_baro', 'ground_speed', 'had_emergency'])

        # Calculate statistics
        max_altitude_baro = df['altitude_baro'].max()
        max_ground_speed = df['ground_speed'].max()
        had_emergency = df['had_emergency'].any()

        # Convert numpy types to Python native types
        max_altitude_baro = float(max_altitude_baro) if not pd.isna(max_altitude_baro) else None
        max_ground_speed = float(max_ground_speed) if not pd.isna(max_ground_speed) else None
        had_emergency = bool(had_emergency)  # Convert numpy.bool_ to Python bool

        # Return the results
        return {
            "max_altitude_baro": max_altitude_baro,
            "max_ground_speed": max_ground_speed,
            "had_emergency": had_emergency
        }
    except Exception as e:
        return {"error": str(e)}

# Example usage
result = get_aircraft_statistics("some_icao_code")
print(result)

# This line should be removed or commented out as it is outside of any function or class
# return {"max_altitude_baro": 300000, "max_ground_speed": 493, "had_emergency": False}
