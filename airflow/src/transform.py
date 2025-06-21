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

def add_hour(df:pd.DataFrame)-> pd.DataFrame:
    df['date'] = pd.to_datetime(df['date'], utc=True)
    df["hour"] = df["date"].dt.strftime('%H:%M:%S')
    return df

def to_enrich(data: pd.DataFrame, storage_path=config['storage']['enriched']) -> None:
    data.to_csv(f"{storage_path}/Weather{str(today_date.date())}.csv", index=False, encoding='utf-8', sep='|')