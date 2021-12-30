import os
import re
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup


def get_info_from_article(article):
    h = dict()
    h["local"] = article.find("p", class_="text-nowrap").text.split(":")[1]

    h["rooms"] = article.find("li", class_="offer-item-rooms hidden-xs").text

    h["price"] = article.find("li", class_="offer-item-price").text
    h["price"] = re.sub("[^0-9]", "", h["price"])

    h["area"] = article.find("li", class_="hidden-xs offer-item-area").text
    h["area"] = re.sub("[m² ]", "", h["area"])

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
        h["restroom"] = aux[0]
        h["restroom"] = re.sub("[^0-9]", "", h["restroom"])
    except:
        h["restroom"] = None

    try:
        h["status"] = aux[1]
    except:
        None

    return h


def get_info_from_page(soup):
    articles = soup.find_all("article")
    aux = []
    for index, article in enumerate(articles):
        try:
            aux.append(get_info_from_article(article))
        except:
            pass
    return aux


def get_regions():
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


def get_number_of_pages(soup):
    try:
        return int(soup.find("ul", class_="pager").find_all("li")[-2].text)
    except:
        return 1


def get_html(region, page, kind="arrendar", movel="apartamento"):
    space = " "
    ifen = "-"
    r = requests.get(
        f"https://www.imovirtual.com/{movel}/{kind}/{region[0].lower().replace(space,ifen)}/?search%5Bregion_id%5D={region[1]}&nrAdsPerPage=72&page={page}"
    )
    return BeautifulSoup(r.text)


def extract_by_type(kind, movel):
    aux = []
    regions = get_regions()
    for region in regions:
        max_pages = get_number_of_pages(get_html(region, 1, kind, movel))
        print(region[0], max_pages)

        for page in range(1, max_pages + 1):
            html = get_html(region, page, kind, movel)
            aux.append(pd.DataFrame(get_info_from_page(html)))

    dataset = pd.concat(aux)
    dataset["kind"] = kind
    dataset["movel"] = movel
    dataset.to_csv(
        os.path.join(os.getenv("OUTPUTS"), f"{kind}_{movel}.csv"), index=False
    )
    return dataset


def download_process_store():
    final = []
    for x in ["moradia", "apartamento"]:
        for y in ["arrendar", "comprar", "ferias"]:
            final.append(extract_by_type(x, y))

    pd.concat([final], axis=1).to_csv(
        os.path.join(os.getenv("OUTPUTS"), "portugal_ads_proprieties.csv"), index=False
    )


download_process_store()
