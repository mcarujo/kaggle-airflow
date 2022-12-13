"""
Kaggle Operator for Airflow.
"""
import os
import shutil
from datetime import datetime

import kaggle
from airflow.models.baseoperator import BaseOperator


class KaggleDatasetPush(BaseOperator):
    """
    Class to create new dataset version directly from the Airflow into Kaggle.
    """

    def __init__(
        self,
        kaggle_dataset: str,
        kaggle_username: str,
        file_name: str,
        output_path: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.kaggle_dataset = kaggle_dataset
        self.kaggle_username = kaggle_username
        self.file_name = file_name
        self.output_path = output_path

    def execute(self, context):
        """
        Creates the path to upload the dataset, download the metadata and push it.
        """
        dataset_path = os.path.join(self.output_path, self.kaggle_dataset)
        if os.path.exists(dataset_path):
            shutil.rmtree(dataset_path)
        os.makedirs(dataset_path)
        shutil.copy(
            os.path.join(self.output_path, self.file_name),
            os.path.join(dataset_path, self.file_name),
        )
        api = kaggle.api
        api.dataset_metadata(
            self.kaggle_dataset,
            dataset_path,
        )
        timestamp = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        api.dataset_create_version(
            dataset_path, f"Updated using airflow at {timestamp}"
        )
