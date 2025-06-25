import pandas as pd
from utils import write_txt
from datetime import datetime
import yaml

with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

today_date = datetime.today()

def import_data(data:list) -> list:
    all_data = []
    
    for response in data:
        # Create dataframe and append to list
        hourly_dataframe = pd.DataFrame(data=response)
        all_data.append(hourly_dataframe)
    return all_data

def convert_to_dataframe(data:list) -> pd.DataFrame:
    return pd.concat(data, ignore_index=True)

def add_day(df:pd.DataFrame) -> pd.DataFrame:
    df['date'] = pd.to_datetime(df['date'], utc=True)
    df["day"] = df["date"].dt.strftime('%Y-%m-%d')
    return df

def add_month(df:pd.DataFrame) -> pd.DataFrame:
    df['date'] = pd.to_datetime(df['date'], utc=True)
    df["mont_num"] = df["date"].dt.month
    return df

def add_hour(df:pd.DataFrame)-> pd.DataFrame:
    df['date'] = pd.to_datetime(df['date'], utc=True)
    df["hour"] = df["date"].dt.strftime('%H:%M:%S')
    return df

def __assign_season__(row)-> str:
    month = row['temperature_2m']
    climate = row['climate']

    if climate == 'temperate':
        if month in [12, 1, 2]:
            return 'Winter'
        elif month in [3, 4, 5]:
            return 'Spring'
        elif month in [6, 7, 8]:
            return 'Summer'
        else:
            return 'Fall'
    elif climate == 'southern_temperate':
        if month in [12, 1, 2]:
            return 'Summer'
        elif month in [3, 4, 5]:
            return 'Fall'
        elif month in [6, 7, 8]:
            return 'Winter'
        else:
            return 'Spring'
    elif climate == 'equatorial':
        return 'Rainy' if month in [4, 5, 10, 11] else 'Dry'
    return 'Unknown'

def assign_climate(df:pd.DataFrame)-> pd.DataFrame:
    df['climate'] = df['city'].map(config['params']['city_climate_map']).fillna('temperate')
    df['season'] = df.apply(__assign_season__, axis=1)
    return df

def to_enrich(data: pd.DataFrame, storage_path=config['storage']['enriched']) -> None:
    data.to_csv(f"{storage_path}/Weather{str(today_date.date())}.csv", index=False, encoding='utf-8', sep='|')