import requests
import time
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
import os
import zipfile
from io import BytesIO
from zipfile import ZipFile

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

def main():

    directory = "downloads"

    if not os.path.exists(directory):
        os.mkdir(directory)
        print(f"Folder '{directory}' has been created! ")
    else:
        print(f"Folder '{directory}' already exists. ")

    for uri in download_uris:
      file_name = os.path.join(directory, os.path.basename(uri))
      response = requests.get(uri, stream=True)
      if response.status_code == 200:
        print(f"{uri} is OK, saving to /{directory} directory.")
        with open(file_name, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
              file.write(chunk)
      else:
        print(f"{uri} is broken, can't be unzipped.")

    zip_files = [file for file in os.listdir(directory) if file.endswith('.zip')]

    for zip_file in zip_files:
        zip_file_path = os.path.join(directory, zip_file)
        
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(directory)
            
        print(f"CSV files extracted from {zip_file}.")

        os.remove(zip_file_path)

        print(f"{zip_file} deleted.")      