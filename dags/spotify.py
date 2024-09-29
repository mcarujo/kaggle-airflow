"""
DAG for -
"""

import logging
import os
from datetime import timedelta

import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.sensors.http_sensor import HttpSensor
from functions.kaggle_operator import KaggleDatasetPush
from functions.kworb_operator import KworbOperator

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


with DAG("spotify_charts", schedule_interval=None, default_args=default_args) as dag:
    KAGGLE_DATASET = "spotify-charts"
    OUTPUT_PATH = os.path.join(Variable.get("ROOT_OUTPUT_PATH"), KAGGLE_DATASET)
    N_JOBS = int(Variable.get("N_JOBS"))
    logging.info("Using OUTPUT_PATH as %s", OUTPUT_PATH)

    is_kworb_available = HttpSensor(
        task_id="is_kworb_available",
        method="GET",
        http_conn_id="http_kworb",
        endpoint="",
        response_check=lambda response: response.status_code == 200,
        poke_interval=5,
        timeout=20,
    )

    task_spotify_charts = KworbOperator(
        task_id="task_spotify_charts",
        base_url="https://kworb.net/spotify/",
        output_path=OUTPUT_PATH,
    )

    task_push_to_kaggle = KaggleDatasetPush(
        task_id="task_spotify_charts_push_to_kaggle",
        kaggle_dataset=KAGGLE_DATASET,  # Must be the output
        kaggle_username="mcarujo",
        output_path=OUTPUT_PATH,
        append=True,
    )
    is_kworb_available >> task_spotify_charts >> task_push_to_kaggle
