import sys
import os
import json
sys.path.append(os.path.dirname(__file__))

import openmeteo_requests
import requests_cache
from retry_requests import retry
import yaml
import pandas as pd
from datetime import datetime
from utils import write_txt

cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)
today_date = datetime.today()

with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

def get_weather_data(config=config) -> list:
    url = config["api"]["base_url"]
    params = {
        "latitude": config["api"]["latitude"],
        "longitude": config["api"]["longitude"],
        "start_date": config["api"]["start_date"],
        "end_date": config["api"]["end_date"],
        "hourly": config["api"]["hourly"],
        "timezone": config["api"]["timezone"]
    }
    
    result = openmeteo.weather_api(url, params=params)
    data = __serializer__(result)
    return data

def __serializer__(data):
    all_data = []
    for response in data:
        # Decodificamos timezone
        decoded_response = response.Timezone().decode('utf-8')
        
        hourly = response.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        hourly_rain = hourly.Variables(1).ValuesAsNumpy()

        # Creamos rango de fechas en UTC
        utc_times = pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        )

        # Convertimos a timezone local
        local_times = utc_times.tz_convert(decoded_response)

        # Armar estructura serializable:
        data_ = {
            "date": local_times.strftime('%Y-%m-%dT%H:%M:%S%z').tolist(),  # Fechas a string ISO
            "temperature_2m": hourly_temperature_2m.tolist(),             # numpy arrays a lista
            "rain": hourly_rain.tolist(),
            "timezone": decoded_response,
            "zone": decoded_response.split('/')[0],
            "city": decoded_response.split('/')[-1]
        }

        all_data.append(data_)
    return all_data

def to_raw(data: list, storage_path=config['storage']['raw']):
    write_txt(storage_path + 'weather_' + str(today_date.date()), data)
    