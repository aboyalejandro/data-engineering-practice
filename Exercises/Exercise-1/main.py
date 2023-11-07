import requests
import time
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
from concurrent.futures import ThreadPoolExecutor
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

        with zipfile.ZipFile(file_name, 'r') as zip_ref:
            zip_ref.extractall(directory)

        print(f"CSV files extracted from {file_name}.")

        os.remove(file_name)
        print(f"{file_name} deleted.")
        
      else:
        print(f"{uri} is broken, can't be unzipped.")