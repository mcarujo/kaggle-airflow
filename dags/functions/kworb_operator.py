import gc  # Import garbage collection module
import os
from datetime import datetime
from typing import List, Tuple

import pandas as pd
import requests
from airflow.models.baseoperator import BaseOperator
from bs4 import BeautifulSoup


class KworbOperator(BaseOperator):
    """
    A class for scraping and processing music charts data from the Kworb website.

    Attributes:
        base_url (str): The base URL for the Kworb website.
        output_path (str): The directory path to store the output CSV files.
    """

    def __init__(
        self,
        base_url: str,
        output_path: str,
        **kwargs,
    ):
        """
        Initializes the KworbOperator with a base URL and output path.

        Args:
            base_url (str): The base URL of the website to scrape.
            output_path (str): The directory where the output CSV files will be stored.

        Raises:
            ValueError: If the base_url is not valid.
        """
        super().__init__(**kwargs)
        if not self._validate_url(base_url):
            raise ValueError(f"Invalid base URL: {base_url}")
        self.base_url = base_url
        self.output_path = output_path

    def _validate_url(self, url: str) -> bool:
        """
        Validates whether a given URL is valid.

        Args:
            url (str): The URL to validate.

        Returns:
            bool: True if the URL is valid, False otherwise.
        """
        try:
            response = requests.head(url)
            return response.status_code == 200
        except requests.RequestException:
            return False

    def fetch_data(self, url: str) -> BeautifulSoup:
        """
        Fetches and parses HTML content from a given URL.

        Args:
            url (str): The relative URL path to append to the base URL.

        Returns:
            BeautifulSoup: Parsed HTML content from the URL.

        Raises:
            Exception: If the request fails.
        """
        full_url = self.base_url + url
        print(f"Requesting data from: {full_url}")
        response = requests.get(full_url)
        if response.status_code == 200:
            return BeautifulSoup(response.content, "html.parser")
        else:
            raise Exception(f"Failed to fetch data from {full_url}: {response.status_code}")

    def extract_table_country_charts_data(self, soup: BeautifulSoup) -> pd.DataFrame:
        """
        Extracts song data from the country-specific charts table in the HTML soup.

        Args:
            soup (BeautifulSoup): Parsed HTML content containing the table.

        Returns:
            pd.DataFrame: DataFrame containing the song data.

        Raises:
            Exception: If the table is not found or has unexpected structure.
        """
        spotify_table = soup.find("table")
        if spotify_table:
            rows = spotify_table.find_all("tr")
            song_data = []
            for row in rows:
                columns = row.find_all("td")
                if len(columns) >= 9:  # If there is no table data means header
                    song_info = {
                        "Position": columns[0].text.strip(),
                        "Change": columns[1].text.strip(),
                        "Artist and Title": columns[2].text.strip(),
                        "Days on Chart": columns[3].text.strip(),
                        "Peak Position": columns[4].text.strip(),
                        "Peak Change": columns[5].text.strip(),
                        "Streams": columns[6].text.strip(),
                        "Streams Change": columns[7].text.strip(),
                        "Total Streams": columns[8].text.strip(),
                    }
                    song_data.append(song_info)
            return pd.DataFrame(song_data)
        else:
            raise Exception("Spotify chart table not found in the HTML content.")

    def extract_table_home_charts_data(self, soup: BeautifulSoup) -> pd.DataFrame:
        """
        Extracts data from the home page table containing country-specific chart links.

        Args:
            soup (BeautifulSoup): Parsed HTML content containing the home table.

        Returns:
            pd.DataFrame: DataFrame with country names and chart links.

        Raises:
            Exception: If the table is not found or the structure is invalid.
        """
        table = soup.find("table")
        if not table:
            raise Exception("Home page table not found.")

        countries = []
        daily_links = []
        weekly_links = []

        for row in table.find_all("tr"):
            cols = row.find_all("td")
            if len(cols) < 2:
                continue

            country_name = cols[0].text.strip()
            links = cols[1].find_all("a")
            if len(links) >= 4:
                daily_link = links[0]["href"]
                daily_total_link = links[1]["href"]
                weekly_link = links[2]["href"]
                weekly_total_link = links[3]["href"]

                countries.append(country_name)
                daily_links.append((daily_link, daily_total_link))
                weekly_links.append((weekly_link, weekly_total_link))
            else:
                raise ValueError(f"Unexpected number of links in table row for country {country_name}.")

        return pd.DataFrame(
            {
                "Country": countries,
                "Daily Link": [link[0] for link in daily_links],
                "Daily Total Link": [link[1] for link in daily_links],
                "Weekly Link": [link[0] for link in weekly_links],
                "Weekly Total Link": [link[1] for link in weekly_links],
            }
        )

    def format_datetime_to_ddmmyyyy(self) -> str:
        """
        Formats the current date as dd-mm-yyyy.

        Returns:
            str: The current date in dd-mm-yyyy format.
        """
        return datetime.now().strftime("%d-%m-%Y")

    def get_table_home(self) -> pd.DataFrame:
        """
        Retrieves the home page table with country-specific chart links.

        Returns:
            pd.DataFrame: DataFrame with country names and chart links.
        """
        return self.extract_table_home_charts_data(self.fetch_data(""))

    def get_table_country(self, url: str) -> pd.DataFrame:
        """
        Retrieves the country-specific chart data from a given URL.

        Args:
            url (str): The relative URL path for the country-specific chart.

        Returns:
            pd.DataFrame: DataFrame containing country-specific chart data.
        """
        return self.extract_table_country_charts_data(self.fetch_data(url))

    def store_table_csv(self, country: str, df: pd.DataFrame):
        """
        Stores the DataFrame containing country chart data as a CSV file.

        Args:
            country (str): The name of the country.
            df (pd.DataFrame): The DataFrame containing the chart data.
        """
        output_dir = os.path.join(self.output_path, self.format_datetime_to_ddmmyyyy())
        os.makedirs(output_dir, exist_ok=True)
        df.to_csv(
            os.path.join(
                output_dir,
                f"spotify_top_charts_weekly_{country.lower().replace(' ', '_')}.csv",
            ),
            index=False,
        )

    def execute(self, **kwargs):
        """
        Fetches and stores chart data for all countries immediately after retrieval to minimize memory usage.
        Each country's chart data is saved as a CSV file right after it's fetched and processed.

        The method fetches the home page table, retrieves data for each country, and saves each
        country's data to a CSV file right away instead of holding all data in memory.

        Garbage collection is also invoked to release memory after each country is processed.
        """
        home_table = self.get_table_home()  # Fetch home table with country-specific links
        output_dir = os.path.join(self.output_path, self.format_datetime_to_ddmmyyyy())
        os.makedirs(output_dir, exist_ok=True)  # Ensure the output directory exists

        for i, line in home_table.iterrows():
            country = line["Country"]
            weekly_link = line["Weekly Link"]

            # Log progress
            print(f"Fetching weekly chart data for {country} from {weekly_link}")

            # Fetch data for the specific country
            country_data = self.get_table_country(weekly_link)

            # Save data immediately after retrieval
            self.store_table_csv(country, country_data)

            # Explicitly delete the DataFrame to free up memory
            del country_data

            # Run garbage collection to free memory
            gc.collect()

            # Log that the file has been saved and memory has been cleared
            print(f"Data for {country} saved successfully. Memory cleared for the next country.")
