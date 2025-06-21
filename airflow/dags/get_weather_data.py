import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..',)))

from src.extract import get_weather_data, to_raw
from src.transform import *
from src.load import load_postgres
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import yaml

with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

default_args = {
    'owner': 'airflow',
    'start_date': '2025-01-01',
    'retries': 1,
}

with DAG(
    dag_id='weather_tets',
    default_args=default_args,
    catchup=False,
    tags=['etl', 'weather', 'get', 'raw']
) as dag:
    t1 = PythonOperator(
        task_id='extract',
        python_callable=get_weather_data
    )
    
    def _save_data(ti):  # ti = task instance
        raw_data = ti.xcom_pull(task_ids='extract')
        return to_raw(data=raw_data, storage_path=config['storage']['raw'])
    
    t2 = PythonOperator(python_callable=_save_data, task_id='save_raw')
    
    t1 >> t2