import logging
import os
from datetime import timedelta

import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.http_sensor import HttpSensor
from functions.kaggle_operator import KaggleDatasetPush
from functions.kaggle_imovirtual_functions import (
    create_output_path,
    serialize_extraction,
    format_transform_consolidate,
)

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
    OUTPUT_PATH = os.path.join(Variable.get("ROOT_OUTPUT_PATH"), "imovirtual")
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

    task_create_output_path = PythonOperator(
        task_id="task_create_output_path",
        python_callable=create_output_path,
        op_kwargs={"output_path": OUTPUT_PATH},
    )

    tasks_download_process_in_series = PythonOperator(
        task_id="tasks_download_process_in_series",
        python_callable=serialize_extraction,
        op_kwargs={
            "output_path_folder": OUTPUT_PATH,
        },
    )

    task_format_transform_consolidate = PythonOperator(
        task_id="task_format_transform_consolidate",
        python_callable=format_transform_consolidate,
        op_kwargs={
            "output_path": OUTPUT_PATH,
            "file_name": "portugal_ads_proprieties.csv",
        },
    )
    task_push_to_kaggle = KaggleDatasetPush(
        task_id="task_push_to_kaggle",
        kaggle_dataset="portugal-proprieties-rent-buy-and-vacation",
        kaggle_username="mcarujo",
        file_name="portugal_ads_proprieties.csv",
        output_path=OUTPUT_PATH,
    )
    (
        is_imovirtual_available
        >> task_create_output_path
        >> tasks_download_process_in_series
        >> task_format_transform_consolidate
        >> task_push_to_kaggle
    )
