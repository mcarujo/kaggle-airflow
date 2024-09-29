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

    Args:
        kaggle_dataset (str): The Kaggle dataset to update.
        kaggle_username (str): The Kaggle username to authenticate with.
        output_path (str): The path where the data to be uploaded is stored.
        append (bool): Whether to append new data to the existing dataset.
    """

    def __init__(
        self,
        kaggle_dataset: str,
        kaggle_username: str,
        output_path: str,
        append: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.kaggle_dataset = kaggle_dataset
        self.kaggle_username = kaggle_username
        self.output_path = output_path
        self.append = append

    def execute(self, context):
        """
        Creates the path to upload the dataset, download the metadata, and push it.
        If append is True, download the existing data from Kaggle, merge it with the new data,
        and push the combined data as a new version.
        """

        if not os.path.exists(self.output_path):
            raise RuntimeError(f"Path '{self.output_path}' does not exist!")

        print("Creating the API connection.")
        api = kaggle.api

        if self.append:
            print("Append mode enabled. Downloading the current version of the dataset.")
            download_path = os.path.join(self.output_path, "existing_data")
            os.makedirs(download_path, exist_ok=True)

            # Download the current dataset version from Kaggle
            api.dataset_download_files(self.kaggle_dataset, path=download_path, unzip=True)

            # Move existing data to the upload directory (local)
            for item in os.listdir(download_path):
                s = os.path.join(download_path, item)
                d = os.path.join(self.output_path, item)
                if os.path.isdir(s):
                    if os.path.exists(d):
                        # If the destination exists, merge directories
                        for sub_item in os.listdir(s):
                            shutil.copy2(os.path.join(s, sub_item), d)  # Copy files
                    else:
                        shutil.copytree(s, d)  # Copy entire directory
                else:
                    shutil.copy2(s, d)  # Copy file

            # Clean up temporary downloaded data
            shutil.rmtree(download_path)

        print("Downloading the metadata.")
        api.dataset_metadata(
            self.kaggle_dataset,
            self.output_path,
        )

        print("Pushing the new version to Kaggle.")
        timestamp = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        api.dataset_create_version(
            self.output_path, f"Updated using Airflow at {timestamp}", delete_old_versions=True, dir_mode="zip"
        )
