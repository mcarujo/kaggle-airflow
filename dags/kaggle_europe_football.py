"""
DAG for https://www.kaggle.com/datasets/mcarujo/portugal-proprieties-rent-buy-and-vacation
"""
import logging
import os
from datetime import timedelta

import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.sensors.http_sensor import HttpSensor
from functions.onefootball_operator import OneFootballOperator

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


with DAG(
    "kaggle_europe_football", schedule_interval=None, default_args=default_args
) as dag:
    OUTPUT_PATH = os.path.join(Variable.get("ROOT_OUTPUT_PATH"), "europe_football")
    logging.info("Using OUTPUT_PATH as %s", OUTPUT_PATH)

    # is_imovirtual_available = HttpSensor(
    #     task_id="is_imovirtual_available",
    #     method="GET",
    #     http_conn_id="http_imovirtual",
    #     endpoint="",
    #     response_check=lambda response: response.status_code == 200,
    #     poke_interval=5,
    #     timeout=20,
    # )

    task_extract_laliga = OneFootballOperator(
        task_id="task_extract_laliga",
        competition_link="https://onefootball.com/en/competition/laliga-10/results",
        competition_name="laliga_2022-23",
        output_path=OUTPUT_PATH,
        chromedriver_path="/opt/airflow/plugins/chromedriver",
    )
    task_extract_laliga
