import requests
import time
from multiprocessing import cpu_count
import concurrent.futures
import os
import zipfile
from io import BytesIO
from zipfile import ZipFile
import logging 

# Defining Logger

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

# Extraction loop

def download_and_extract(uri,directory):
      try:
        file_name = os.path.join(directory, os.path.basename(uri))
        response = requests.get(uri, stream=True)
        if response.status_code == 200:
          logger.info(f"{uri} is OK, saving to /{directory} directory.")

        # Writing .zip into /downloads

          with open(file_name, 'wb') as file:
              for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        # Extracting .csv from .zip

          with zipfile.ZipFile(file_name, 'r') as zip_ref:
              zip_ref.extractall(directory)

          logger.info(f"CSV files extracted from {file_name}.")

        # Removing .zip from /downloads

          os.remove(file_name)

          logger.info(f"{file_name} deleted.")
        
        else:
          logger.error(f"{uri} is broken, can't be unzipped.")
        
      except Exception as err:
        print(err)


def main():

    directory = "downloads"

    # Checking if directory already exists

    if not os.path.exists(directory):
        os.mkdir(directory)
        logger.info(f"Folder '{directory}' has been created!")
    else:
        logger.info(f"Folder '{directory}' already exists.")

    # Setting up ThreadPool Executor

    with concurrent.futures.ThreadPoolExecutor(max_workers=cpu_count()) as executor:
        futures = [executor.submit(download_and_extract, uri, directory) for uri in download_uris]
        concurrent.futures.wait(futures)
      
if __name__ == "__main__":
    main()