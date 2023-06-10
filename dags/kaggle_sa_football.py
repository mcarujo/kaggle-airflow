"""
DAG for https://www.kaggle.com/datasets/mcarujo/south-america-football-season-2023
"""
import logging
import os
from datetime import timedelta

import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.sensors.http_sensor import HttpSensor
from functions.kaggle_operator import KaggleDatasetPush
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
    "kaggle_south-america-football", schedule_interval=None, default_args=default_args
) as dag:
    KAGGLE_DATASET = "south-america-football-season-2023"
    OUTPUT_PATH = os.path.join(Variable.get("ROOT_OUTPUT_PATH"), KAGGLE_DATASET)
    N_JOBS = int(Variable.get("N_JOBS"))
    logging.info("Using OUTPUT_PATH as %s", OUTPUT_PATH)

    is_onefootball_available = HttpSensor(
        task_id="is_onefootball_available",
        method="GET",
        http_conn_id="http_onefootball",
        endpoint="",
        response_check=lambda response: response.status_code == 200,
        poke_interval=5,
        timeout=20,
    )

    competitions = [
        {
            "competition_link": "https://onefootball.com/en/competition/primera-division-de-argentina-183/results",
            "competition": "primera-division-de-argentina-23",
        },
        {
            "competition_link": "https://onefootball.com/en/competition/primera-division-82/results",
            "competition": "primera-division-cl-23",
        },
        {
            "competition_link": "https://onefootball.com/en/competition/brasileirao-serie-a-16/results",
            "competition": "brasileirao-serie-a-23",
        },
        {
            "competition_link": "https://onefootball.com/en/competition/conmebol-sudamericana-102/results",
            "competition": "conmebol-sudamericana-23",
        },
        {
            "competition_link": "https://onefootball.com/en/competition/conmebol-libertadores-76/results",
            "competition": "conmebol-libertadores-23",
        },
    ]
    tasks_onefootball = []
    for i, competition in enumerate(competitions):
        tasks_onefootball.append(
            OneFootballOperator(
                task_id=f'task_extract_{competition["competition"]}',
                competition_link=competition["competition_link"],
                competition_name=competition["competition"],
                output_path=OUTPUT_PATH,
                chromedriver_path="/opt/airflow/plugins/chromedriver",
                n_jobs=N_JOBS,
            )
        )
        if i == 0:
            is_onefootball_available >> tasks_onefootball[0]
        else:
            tasks_onefootball[i - 1] >> tasks_onefootball[i]

    task_push_to_kaggle = KaggleDatasetPush(
        task_id="task_push_to_kaggle",
        kaggle_dataset=KAGGLE_DATASET,  # Must be the output
        kaggle_username="mcarujo",
        output_path=OUTPUT_PATH,
    )
    tasks_onefootball[-1] >> task_push_to_kaggle
