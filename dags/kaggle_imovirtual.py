"""
DAG for https://www.kaggle.com/datasets/mcarujo/portugal-proprieties-rent-buy-and-vacation
"""
import logging
import os
from datetime import timedelta

import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from functions.kaggle_operator import KaggleDatasetPush
from functions.imovirtual_functions import imovirtual_extract_transform

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
    KAGGLE_DATASET = "portugal-proprieties-rent-buy-and-vacation"
    OUTPUT_PATH = os.path.join(Variable.get("ROOT_OUTPUT_PATH"), KAGGLE_DATASET)
    logging.info("Using OUTPUT_PATH as %s", OUTPUT_PATH)

    is_imovirtual_available = HttpSensor(
        task_id="is_imovirtual_available",
        method="GET",
        http_conn_id="http_imovirtual",
        endpoint="",
        response_check=lambda response: response.status_code == 200,
        poke_interval=5,
        timeout=20,
    )

    imovirtual_extract_transform = PythonOperator(
        task_id="imovirtual_extract_transform",
        python_callable=imovirtual_extract_transform,
        op_kwargs={
            "file_name": "portugal_ads_proprieties.csv",
            "output_path": OUTPUT_PATH,
            "count_try": 3,
            "break_time": 300,  # 300 seconds = 5 minutes
        },
    )
    task_push_to_kaggle = KaggleDatasetPush(
        task_id="task_push_to_kaggle",
        kaggle_dataset=KAGGLE_DATASET,
        kaggle_username="mcarujo",
        output_path=OUTPUT_PATH,
    )
    (is_imovirtual_available >> imovirtual_extract_transform >> task_push_to_kaggle)
