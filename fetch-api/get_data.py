import os
import requests
from io import BytesIO
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MAIN_DIR = os.path.dirname(BASE_DIR)

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
YELLOW_TAXI = "yellow_tripdata_{year}-{month}.parquet"
GREEN_TAXI = "green_tripdata_{year}-{month}.parquet"
FOR_HIRED_VEHICLE = "fhv_tripdata_{year}-{month}.parquet"
HIGH_VOLUME_VEHICLE = "fhvhv_tripdata_{year}-{month}.parquet"

def get_data(year, month, taxi_type):
    if taxi_type == "yellow":
        url = BASE_URL + YELLOW_TAXI.format(year=year, month=month)
    elif taxi_type == "green":
        url = BASE_URL + GREEN_TAXI.format(year=year, month=month)
    elif taxi_type == "fhv":
        url = BASE_URL + FOR_HIRED_VEHICLE.format(year=year, month=month)
    elif taxi_type == "fhvhv":
        url = BASE_URL + HIGH_VOLUME_VEHICLE.format(year=year, month=month)
    else:
        raise ValueError("Invalid taxi type. Please choose from yellow, green, fhv, fhvhv.")
    
    print("Fetching data from", url)
    
    response = requests.get(url)
    if response.status_code == 200:
        os.makedirs(f'{MAIN_DIR}/api-data/{taxi_type}/{year}', exist_ok=True)  
        file_path = f"{MAIN_DIR}/api-data/{taxi_type}/{year}/{taxi_type}_{year}_{month}.parquet"
        parquet_file = BytesIO(response.content)
        df = pd.read_parquet(parquet_file, engine='pyarrow')
        df.to_parquet(file_path, index=False)
        print(f"Parquet file has been saved locally as '{file_path}'")
        return file_path
    else:
        print("Data not found")

def get_all_data(year, month):
    yellow_data = get_data(year, month, "yellow")
    green_data = get_data(year, month, "green")
    fhv_data = get_data(year, month, "fhv")
    fhvhv_data = get_data(year, month, "fhvhv")
    
    return yellow_data, green_data, fhv_data, fhvhv_data

def main():
    year = 2019
    month = '01'
    yellow_data, green_data, fhv_data, fhvhv_data = get_all_data(year, month)
    print("Data saved to:")
    print(yellow_data)
    print(green_data)
    print(fhv_data)
    print(fhvhv_data)
    
    # print(MAIN_DIR)

if __name__ == "__main__":
    main()
