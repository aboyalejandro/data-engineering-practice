import requests
import pandas as pd
import logging 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():

    url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'

    logger.info("Getting the HTML content")

    ncei_climatedata=pd.read_html(str(requests.get(url).content))
    ncei_climatedata=pd.DataFrame(ncei_climatedata[0])

    logger.info("Dropping useless columns.")

    ncei_climatedata.drop(['Size','Description'], inplace=True, axis=1)

    # Taking the first ocurrence because there are 102 files with that Last modified

    csv = list(ncei_climatedata['Name'][ncei_climatedata['Last modified'] == '2022-02-07 14:03'])[0]

    # Building the URL 
    logger.info("Getting the .csv file and adding it to the URL to make the request")

    url = f'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/{csv}'
    document = pd.read_csv(url)

    logger.info("Cleaning dataset and converting types")

    document = document.drop_duplicates()
    document.convert_dtypes()

    document['DATE'] = pd.to_datetime(document['DATE'])

    logger.info("Getting the highest values of HourlyDryBulbTemperature")

    document[['STATION',
            'DATE',
            'NAME',
            'REPORT_TYPE',
            'HourlyDryBulbTemperature']] \
            [document['HourlyDryBulbTemperature'] > document['HourlyDryBulbTemperature'].median()] \
            .sort_values(by=['DATE'], ascending=False)
    
if __name__ == "__main__":
    main()
