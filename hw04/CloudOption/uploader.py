import requests
from google.cloud import storage
import os

# User input
json_file = "gcs.json"  # Enter your Google App Credential JSON with storage access
bucket_name = "dezoomcamp_hw04_2025" # Enter your Google Storage bucket name

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_file

def initialize_storage_client():
    """Initialize a Google Cloud Storage client."""
    try:
        storage_client = storage.Client()
        print("Storage client initialized.")
        return storage_client
    except Exception as e:
        print(f"Error initializing storage client: {e}")
        raise

def get_bucket(storage_client, bucket_name):
    """Retrieve the specified bucket."""
    try:
        bucket = storage_client.get_bucket(bucket_name)
        print(f"Bucket {bucket_name} accessed.")
        return bucket
    except Exception as e:
        print(f"Error accessing bucket {bucket_name}: {e}")
        raise

def download_and_upload_data(bucket, taxi_type, base_url, year, month):
    """Download data from a URL and upload it to the specified GCS bucket."""
    url = f"{base_url}{year}-{month}.csv.gz"
    try:
        print(f"Fetching data from: {url}")
        response = requests.get(url, timeout=500)
        response.raise_for_status()
        blob = bucket.blob(f"{taxi_type}_tripdata_{year}-{month}.csv.gz")
        print("Uploading to GCS")
        blob.upload_from_string(response.content)
        print(f"Successfully uploaded {taxi_type}_tripdata_{year}-{month}.csv.gz to GCS")
    except requests.RequestException as e:
        print(f"Error downloading {url}: {e}")
    except Exception as e:
        print(f"An error occurred while processing {url}: {e}")

# Initialize the storage client
storage_client = initialize_storage_client()

# Get the bucket
bucket = get_bucket(storage_client, bucket_name)

# Define the base URLs for the data
base_urls = {
    'yellow': 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_',
    'green': 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_',
    'fhv': 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_'
}

# Define the years and months to download
years = ['2019', '2020']
months = [str(month).zfill(2) for month in range(1, 13)]

# Download the data and upload to GCS
for taxi_type, base_url in base_urls.items():
    print(f"Processing taxi type: {taxi_type}")
    for year in years:
        for month in months:
            if taxi_type == 'fhv' and year == '2020':
                print(f"Skipping FHV data for year {year}")
                continue
            download_and_upload_data(bucket, taxi_type, base_url, year, month)
