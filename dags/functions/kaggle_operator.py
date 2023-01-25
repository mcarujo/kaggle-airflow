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
        output_path: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.kaggle_dataset = kaggle_dataset
        self.kaggle_username = kaggle_username
        self.output_path = output_path

    def execute(self, context):
        """
        Creates the path to upload the dataset, download the metadata and push it.
        """

        if not os.path.exists(self.output_path):
            raise RuntimeError(f"Path '{self.output_path}' do not exists!")

        print("Creating the API connection.")
        api = kaggle.api

        print("Downloading the metadata.")
        api.dataset_metadata(
            self.kaggle_dataset,
            self.output_path,
        )

        print("Pushing the new version to Kaggle.")
        timestamp = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        api.dataset_create_version(
            self.output_path, f"Updated using airflow at {timestamp}"
        )
