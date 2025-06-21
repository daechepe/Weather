import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..',)))

from src.extract import get_weather_data, to_raw
from src.transform import *
from src.load import *
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import yaml

today_date = datetime.today()
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

default_args = {
    'owner': 'airflow',
    'start_date': '2025-01-01',
    'retries': 1,
}

with (DAG(
    dag_id='weather_etl',
    default_args=default_args,
    catchup=False,
    tags=['etl', 'weather']
) as dag):
    extract_data = PythonOperator(
        task_id='extract',
        python_callable=get_weather_data
    )
    
    def _save_data(ti):  # ti = task instance
        raw_data = ti.xcom_pull(task_ids='extract')
        return to_raw(raw_data)
    
    save_to_raw = PythonOperator(python_callable=_save_data, task_id='save_raw')

    def _call_extract(ti):
        raw_data = ti.xcom_pull(task_ids='extract')
        return import_data(raw_data)

    editable_format = PythonOperator(
        task_id='convert_to_editable_format',
        python_callable=_call_extract
    )

    def _create_dataframe(ti):
        raw_data = ti.xcom_pull(task_ids='convert_to_editable_format')
        return convert_to_dataframe(raw_data)

    create_table = PythonOperator(
        task_id='create_Dataframe',
        python_callable=_create_dataframe
    )

    def _enrich(ti):
        enrich_data = ti.xcom_pull(task_ids='create_Dataframe')
        return to_enrich(enrich_data)

    save_enrich = PythonOperator(task_id='to_enrich', python_callable=_enrich)

    def _get_dataframe(ti):
        data = ti.xcom_pull(task_ids='create_Dataframe')
        return add_day(data)
    
    get_day_from_datetime = PythonOperator(
        task_id='get_day',
        python_callable=_get_dataframe
    )

    def _get_dataframe_day(ti):
        data = ti.xcom_pull(task_ids='get_day')
        return add_hour(data)
    
    get_hour_from_datetime = PythonOperator(
        task_id='get_hour',
        python_callable=_get_dataframe_day
    )

    def _get_final_df(ti):
        data = ti.xcom_pull(task_ids='get_hour')
        save_parquet(data, config['storage']['processed'] + "Weather.parquet")
    
    save_processed = PythonOperator(
        task_id='load',
        python_callable=_get_final_df
    )

    extract_data >> [save_to_raw, editable_format]
    editable_format >> [create_table, save_enrich]
    create_table >> get_day_from_datetime >> get_hour_from_datetime >> save_processed