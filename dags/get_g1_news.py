from datetime import datetime, timedelta

import airflow
import requests
from airflow import DAG
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.python_operator import PythonOperator


# SETTINGS
default_args = {
            "owner": "Airflow",
            "start_date": airflow.utils.dates.days_ago(1),
            "depends_on_past": False,
            "email_on_failure": True,
            "email_on_retry": False,
            "email": "mamcarujo@gmail.com",
            "retries": 1,
            "retry_delay": timedelta(minutes=15)
        }

# FUNCTIONS
def download_g1_webpage():
    response = requests.get("https://g1.globo.com")
    file = open("g1_sample.html","w")
    file.write(response.text)
    file.close()

# DAG
with DAG('g1_example', schedule_interval="@daily", default_args=default_args, catchup=False) as dag:
    
    # airflow tasks test g1_example is_g1_available 2021-01-01
    is_g1_available = HttpSensor(
            task_id="is_g1_available",
            method="GET",
            http_conn_id="http_g1",
            endpoint="",
            response_check=lambda response: response.status_code == 200,
            poke_interval=5,
            timeout=20
    )

    downloading_g1_webpage = PythonOperator(
            task_id="downloading_g1_webpage",
            python_callable=download_g1_webpage
    )

    is_g1_available >> downloading_g1_webpage

