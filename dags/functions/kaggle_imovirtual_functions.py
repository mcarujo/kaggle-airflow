"""
Helper file for imovirtual DAG.
"""

import logging
import os
import re
import shutil
import time

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup


def get_info_from_article(article: BeautifulSoup) -> dict:
    """
    Receive a soup object and html then return in a dict the data.

    Args:
        article (BeautifulSoup): soup object that contains the ads html.

    Returns:
        dict: information related to the announcement.
    """

    residence = dict()
    residence["local"] = article.find("p", class_="text-nowrap").text.split(":")[1]

    residence["rooms"] = article.find("li", class_="offer-item-rooms hidden-xs").text

    residence["price"] = article.find("li", class_="offer-item-price").text
    residence["price"] = re.sub("[^0-9]", "", residence["price"])

    residence["area"] = article.find("li", class_="hidden-xs offer-item-area").text
    residence["area"] = re.sub("[m² ]", "", residence["area"])

    try:
        aux = [
            li.text
            for li in article.find("ul", class_="parameters-view hidden-xs").find_all(
                "li"
            )
        ]

    except:
        aux = [
            li.text
            for li in article.find(
                "ul", class_="params-small clearfix hidden-xs"
            ).find_all("li")
        ]
    try:
        residence["restroom"] = re.sub("[^0-9]", "", aux[0])
    except:
        residence["restroom"] = None

    try:
        residence["status"] = aux[1]
    except:
        residence["status"] = np.nan

    return residence


def get_info_from_page(soup: BeautifulSoup) -> list:
    """Create a list of ads for each page.

    Args:
        soup (BeautifulSoup): html of a page that contains a list of ads.

    Returns:
        list: returns a list of dict with the ad information.
    """
    articles = soup.find_all("article")
    aux = []
    for index, article in enumerate(articles):
        try:
            aux.append(get_info_from_article(article))
        except:
            logging.info("Error to get article index %s", index)
    return aux


def get_regions():
    """
    Store the Regions of Portugal and the directly ID.

    Returns:
        list:tuple: return a list of tuples for Region and Region id.
    """
    return [
        ("Aveiro", "1"),
        ("Beja", "2"),
        ("Braga", "3"),
        ("Bragança", "4"),
        ("Castelo Branco", "5"),
        ("Coimbra", "6"),
        ("Évora", "7"),
        ("Faro", "8"),
        ("Guarda", "9"),
        ("Ilha da Graciosa", "24"),
        ("Ilha da Madeira", "19"),
        ("Ilha das Flores", "28"),
        ("Ilha de Porto Santo", "20"),
        ("Ilha de Santa Maria", "21"),
        ("Ilha de São Jorge", "25"),
        ("Ilha de São Miguel", "22"),
        ("Ilha do Corvo", "29"),
        ("Ilha do Faial", "27"),
        ("Ilha do Pico", "26"),
        ("Ilha Terceira", "23"),
        ("Leiria", "10"),
        ("Lisboa", "11"),
        ("Portalegre", "12"),
        ("Porto", "13"),
        ("Santarém", "14"),
        ("Setúbal", "15"),
        ("Viana do Castelo", "16"),
        ("Vila Real", "17"),
        ("Viseu", "18"),
    ]


def get_number_of_pages(soup: BeautifulSoup) -> int:
    """
    Catch the information of how many pages of ad for that area.

    Args:
        soup (BeautifulSoup): soup object that contains the html.

    Returns:
        int: The number of pages, in case of issue returns 1.
    """
    try:
        return int(soup.find("ul", class_="pager").find_all("li")[-2].text)
    except:
        return 1


def get_html_as_bs(
    region: tuple, page: str, service_type: str, residence_type: str
) -> BeautifulSoup:
    """
    Execute a request to the website then use the html to instantiate a BeautifulSoup object.

    Args:
        region (tuple): region of Portugal used in the search.
        page (str): searched page.
        service_type (str): determine what service the residence belongs.
        residence_type (str): the type of residence, covered so far by house or apartment.

    Returns:
        BeautifulSoup: a BeautifulSoup object made using the html requested.
    """
    space = " "
    dash = "-"
    response = requests.get(
        f"https://www.imovirtual.com/{service_type}/{residence_type}"
        + f"/{region[0].lower().replace(space,dash)}"
        + f"/?search%5Bregion_id%5D={region[1]}&nrAdsPerPage=72&page={page}",
        timeout=120,
    )
    return BeautifulSoup(response.text, features="lxml")


def serialize_extraction(output_path_folder: str) -> None:
    """
    Start the data extraction process based on residence and service type.

    Args:
        output_path_folder (str): path to store the partial csv.
    """
    for residence_type in ["moradia", "apartamento"]:  # house or apartment
        for service_type in ["arrendar", "comprar", "ferias"]:  # rent, buy or vacation
            extract_by_type(service_type, residence_type, output_path_folder)


def extract_by_type(
    service_type: str, residence_type: str, output_path: str, time_sleep: int = 1
) -> None:
    """
    Extract the data from imovirtual page based on service and residence type
    then store a DataFrame in the output folder specified.

    Args:
        service_type (str): determine what service the residence belongs.
        residence_type (str): the type of residence, covered so far by house or apartment.
        output_path (str): the DataFrame destination.
        time_sleep (int): seconds to wait between requests.

    Returns:
        None: the goal is just store in the output folder.
    """
    logging.info(
        "Started to extract data based on  %s -  %s !", service_type, residence_type
    )
    pages = []
    regions = get_regions()
    for region in regions:
        max_pages = get_number_of_pages(
            get_html_as_bs(region, "1", service_type, residence_type)
        )
        logging.info(
            "Total of %s pages for the %s region considering %s - %s.",
            max_pages,
            region[0],
            service_type,
            residence_type,
        )

        for page in range(1, max_pages + 1):
            time.sleep(time_sleep)
            html = get_html_as_bs(region, page, service_type, residence_type)
            pages.append(pd.DataFrame(get_info_from_page(html)))

    dataset = pd.concat(pages)
    dataset["service_type"] = service_type
    dataset["residence_type"] = residence_type
    df_output_path = os.path.join(
        output_path,
        f"{service_type}_{residence_type}.csv",
    )
    logging.info("The DataFrame will be stored at %s", df_output_path)
    dataset.to_csv(df_output_path, index=False)
    logging.info(
        "Finished to extract data based on %s - %s!", service_type, residence_type
    )


def create_output_path(output_path: str):
    """
    Function to create the output if not exists.

    Args:
        output_path (str): output folder name.
    """
    if os.path.exists(output_path):
        logging.info(
            "The output path '%s' already exists, so let's clean...", output_path
        )
        shutil.rmtree(output_path)

    os.makedirs(output_path)
    logging.info("The output path '%s' has been created empty.", output_path)


def format_transform_consolidate(output_path: str, file_name: str) -> None:
    """Load all partials csv into one, format it and then store it.

    Args:
        output_path (str): path to store the final csv.
        file_name (str): name for the final csv.

    Returns:
        None: no return.
    """
    paths_df = os.listdir(output_path)
    list_df = pd.concat(
        [pd.read_csv(os.path.join(output_path, path_df)) for path_df in paths_df]
    )
    list_df.columns = [
        "Location",
        "Rooms",
        "Price",
        "Area",
        "Bathrooms",
        "Condition",
        "AdsType",
        "ProprietyType",
    ]
    list_df.Price = list_df.Price.apply(float).round(2)
    list_df.Rooms = list_df.Rooms.apply(lambda x: x.replace("T", ""))

    def format_bathrooms(quantity_bathrooms):
        try:
            bathrooms = int(quantity_bathrooms)
            if bathrooms > 100:
                return np.nan
            return bathrooms
        except:
            return np.nan

    list_df.Bathrooms = list_df.Bathrooms.apply(format_bathrooms)

    list_df.Rooms = list_df.Rooms.apply(lambda x: x.replace("T", ""))
    list_df.Condition = list_df.Condition.map(
        {
            "Ruína": "In ruin",
            "Novo": "New",
            "Renovado": "Renovated",
            "Usado": "Used",
            "Em construção": "Under construction",
            "Para recuperar": "To recovery",
        }
    )
    list_df.AdsType = list_df.AdsType.map(
        {
            "arrendar": "Rent",
            "ferias": "Vacation",
            "comprar": "Sell",
        }
    )
    list_df.ProprietyType = list_df.ProprietyType.map(
        {
            "apartamento": "Apartment",
            "moradia": "House",
        }
    )
    list_df.Area = list_df.Area.str.replace(",", ".").apply(float).round(2)
    list_df.dropna()
    list_df.to_csv(os.path.join(output_path, file_name), index=False)
