from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.http_sensor import HttpSensor

from functions.kaggle_imovirtual_functions import download_process_store

# SETTINGS
default_args = {
    "owner": "Airflow",
    "start_date": airflow.utils.dates.days_ago(1),
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": "mamcarujo@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG("kaggle_imovirtual", schedule_interval=None, default_args=default_args) as dag:

    is_imovirtual_available = HttpSensor(
        task_id="is_imovirtual_available",
        method="GET",
        http_conn_id="http_imovirtual",
        endpoint="",
        response_check=lambda response: response.status_code == 200,
        poke_interval=5,
        timeout=20,
    )

    do_download_process_store = PythonOperator(
        task_id="do_download_process_store", python_callable=download_process_store
    )

    is_imovirtual_available >> do_download_process_store
