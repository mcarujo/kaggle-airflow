"""
OneFootball Operator for Airflow.
"""
import os
import re
import time
from datetime import datetime, timedelta

import pandas as pd
from airflow.models import Variable
from airflow.models.baseoperator import BaseOperator
from bs4 import BeautifulSoup
from joblib import Parallel, delayed
from selenium import webdriver
from selenium.webdriver.common.by import By


class OneFootballOperator(BaseOperator):
    """
    Class to create new dataset version directly from the Airflow into Kaggle.
    """

    def __init__(
        self,
        competition_link: str,
        competition_name: str,
        output_path: str,
        chromedriver_path: str,
        n_jobs: int = 1,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.competition_link = competition_link
        self.competition_name = competition_name
        self.output_path = output_path
        self.chromedriver_path = chromedriver_path
        self.n_jobs = n_jobs

    def execute(self, **kwargs):
        """
        Creates the path to upload the dataset, download the metadata and push it.
        """
        print("SAVING THE TABLE")
        page = self.get_html_table(self.competition_link)
        self.get_info_html_table(page)
        print("EXTRACTING")
        df_competition = self.extraction(self.competition_link)
        print("TRANSFORMING")
        df_matches, df_events = self.transform(df_competition)
        print("SAVING")
        self.store(df_matches, df_events, self.competition_name, self.output_path)

    def browser(self, url):
        option = webdriver.ChromeOptions()
        option.add_argument("--headless")
        option.add_argument("--no-sandbox")
        option.add_argument("--disable-dev-shm-usage")

        driver = webdriver.Chrome(
            executable_path=self.chromedriver_path,
            options=option,
        )
        driver.get(url)
        return driver

    def get_html_full(self, url):
        driver = self.browser(url)
        time.sleep(5)
        el = driver.find_element(
            By.XPATH, "/html/body/div/div[2]/div/div[1]/div/div[2]/div/button[1]"
        )
        driver.execute_script("arguments[0].click();", el)
        el = driver.find_element(
            By.XPATH,
            "/html/body/of-root/div/main/of-entity-stream/section/of-xpa-layout-entity/section[5]/of-xpa-switch-entity/section/of-match-cards-lists-appender/div/div/button/span",
        )
        driver.execute_script("arguments[0].click();", el)
        time.sleep(5)
        page = driver.page_source
        driver.close()
        return BeautifulSoup(page, features="lxml")

    def get_html_table(self, url):
        driver = self.browser(url.replace("results", "table"))
        time.sleep(5)
        el = driver.find_element(
            By.XPATH, "/html/body/div/div[2]/div/div[1]/div/div[2]/div/button[1]"
        )
        driver.execute_script("arguments[0].click();", el)
        return BeautifulSoup(driver.page_source, features="lxml")

    def get_table(self, article):
        lines = article.find("ul").find_all(
            "li", class_="standings__row standings__row--link"
        )
        line_values = []
        for line in lines:
            aux_values = []
            aux_values.append(
                line.find("p", class_="title-7-medium standings__team-name").text
            )
            for div in line.find_all("div", class_="standings__cell"):
                aux_values.append(div.text)
            line_values.append(aux_values)
        table = pd.DataFrame(line_values)
        table.columns = ["Team", "Position", "drop", "PL", "W", "D", "L", "GD", "PTS"]
        table.drop("drop", axis=1, inplace=True)
        return table

    def get_info_html_table(self, page):
        articles = page.find_all("article", class_="standings__table-wrapper")

        if len(articles) < 1:
            table = self.get_table(articles[0])
        else:
            aux_table_list = []
            for article in articles:
                group_name = article.find(
                    "p", class_="standings__table-header-text"
                ).text
                table = self.get_table(article)
                table["group_name"] = group_name
                aux_table_list.append(table)
            table = pd.concat(aux_table_list)

        table.to_csv(
            os.path.join(self.output_path, f"table_{self.competition_name}.csv"),
            index=False,
        )

    def get_html_full_match(self, url):
        driver = self.browser(url)
        el = driver.find_element_by_xpath(
            "/html/body/of-root/main/of-competition-results-stream/section/of-entity-page-root/div/of-xpa-switch-entity-deprecated[7]/of-simple-match-cards-list-deprecated/div/button"
        )
        driver.execute_script("arguments[0].click();", el)
        time.sleep(5)
        page = driver.page_source
        driver.close()
        return BeautifulSoup(page, features="lxml")

    def get_html_from_a_match(self, url):
        driver = self.browser(url)
        time.sleep(3)
        # Cache or cookies
        try:
            el = driver.find_element_by_xpath(
                "/html/body/div/div[2]/div/div[1]/div/div[2]/div/button[1]"
            )
            driver.execute_script("arguments[0].click();", el)
            time.sleep(3)
        except:
            pass
        # Show all match events
        try:
            el = driver.find_element(
                By.XPATH, "//*[@id='__next']/main/div/div/div[5]/div/div/button"
            )
            driver.execute_script("arguments[0].click();", el)
            time.sleep(1)
        except:
            try:
                el = driver.find_element(
                    By.XPATH, "//*[@id='__next']/main/div/div/div[4]/div/div/button"
                )
                driver.execute_script("arguments[0].click();", el)
                time.sleep(1)
            except:
                pass
        page = driver.page_source
        # Away lineup
        try:
            el = driver.find_element_by_xpath(
                "/html/body/div[1]/main/div/div/div[5]/div[1]/div/section/nav/ul/li[2]/button/div/span"
            )
        except:
            el = driver.find_element_by_xpath(
                "/html/body/div[1]/main/div/div/div[6]/div[1]/div/section/nav/ul/li[2]/button/div/span"
            )
        driver.execute_script("arguments[0].click();", el)
        time.sleep(1)
        second_lineup = driver.page_source
        driver.close()
        return BeautifulSoup(page, features="lxml"), BeautifulSoup(
            second_lineup, features="lxml"
        )

    def format_field(self, string):
        string = str(string)
        string = string.lower()
        string = string.strip()
        string = string.replace("  ", " ")
        string = string.replace(" ", "_")
        return string

    def indentify_event(self, text):
        events = [
            "yellow-card",
            "red-card",
            "goal",
            "own-goal",
            "substitution",
            "penalty",
        ]
        for event in events:
            if event in text:
                return event
        return ""

    def get_info_from_event(self, event):
        aux_event = {}
        aux_event["event_team"] = event["class"][1].split("__")[0][-4:]
        aux_event["event_time"] = event.find(
            "p", class_=re.compile("MatchEventsTimeline_matchEventsItemTimeline")
        ).text

        if aux_event["event_time"] == " PK ":
            aux_event["event_time"] = False
            aux_event["event_type"] = "PK"
            aux_event["event_result"] = event.find("img", class_="of-image__img")["alt"]
            aux_event["event_player"] = (
                event.find("div", class_=re.compile("MatchEventCard_matchEventsText"))
                .find("p")
                .text
            )
        else:
            aux_event["event_type"] = self.indentify_event(
                event.find("source")["srcset"]
            )
            try:
                for i, text in enumerate(
                    event.find(
                        "div", class_=re.compile("MatchEventCard_matchEventsText")
                    ).find_all("p")
                ):
                    aux_event["action_player_" + str(i + 1)] = text.text
            except:
                aux_event["action_player_1"] = event.find(
                    "p", class_=re.compile("MatchEventCard_matchEventsText")
                ).text

        return aux_event

    def get_info_from_match(self, page, second_lineup):
        aux_dict = {}

        ## NAMES
        aux_dict["team_name_home"] = page.find(
            "a", class_=re.compile("MatchScoreTeam_home")
        ).text
        aux_dict["team_name_away"] = page.find(
            "a", class_=re.compile("MatchScoreTeam_away")
        ).text

        ## GOAL
        scores = page.find("p", class_=re.compile("MatchScore_scores")).find_all("span")
        aux_dict["team_home_score"] = scores[0].text
        aux_dict["team_away_score"] = scores[2].text

        ## PENS
        try:
            pens = (
                page.find("div", class_=re.compile("MatchScore_data"))
                .find("span", class_="title-7-medium")
                .text
            )
            if "Pens" in pens:
                aux_dict["pens"] = True
                pens_aux = pens.split(": ")[1].split(" - ")
                aux_dict["pens_home_score"] = pens_aux[0]
                aux_dict["pens_away_score"] = pens_aux[1]
        except:
            pass
        ## STATISTICS
        try:
            description = page.find_all(
                "div", class_=re.compile("MatchStatsEntry_description")
            )
            for d in description:
                field = self.format_field(d.find_all("p")[1].text)
                aux_dict[field + "_home"] = d.find_all("p")[0].text
                aux_dict[field + "_away"] = d.find_all("p")[2].text
        except:
            pass

        ## PREDICTION
        predictions = page.find(
            "ul", class_=re.compile("MatchPrediction_buttons")
        ).find_all("li")
        aux_dict["prediction_team_home_win"] = predictions[0].text
        aux_dict["prediction_draw"] = predictions[1].text
        aux_dict["prediction_team_away_win"] = predictions[2].text
        aux_dict["prediction_quantity"] = (
            page.find("p", class_=re.compile("MatchPrediction_message")).find("b").text
        )

        ## LOCATION
        entries = page.find("ul", class_=re.compile("MatchInfo_entries")).find_all("li")
        aux_dict["location"] = (
            entries[-1].find("span", class_=re.compile("MatchInfoEntry_subtitle")).text
        )
        aux_dict["date"] = (
            entries[1].find("span", class_=re.compile("MatchInfoEntry_subtitle")).text
        )

        ## EVENTS
        events = page.find(
            "ul", class_=re.compile("MatchEvents_matchEventsList")
        ).find_all("li", class_=re.compile("MatchEvents_matchEventsItem"))
        aux_dict["events_list"] = [self.get_info_from_event(event) for event in events]

        aux_dict["lineup_home"] = self.get_lineups(page)
        aux_dict["lineup_away"] = self.get_lineups(second_lineup)

        return aux_dict

    def get_lineups(self, page):
        lineup = []
        for player in page.find_all(
            "span", class_=re.compile("MatchLineupFormation_playerNameText")
        ):
            player_info = player.text.split(".")
            lineup.append(
                {
                    "player_name": player_info[1],
                    "player_number": player_info[0],
                }
            )
        return lineup

    def get_penalties(self, match):
        regex_rule = r"\((\d+)\)"
        aux = match.find_all(
            "span", class_="title-7-bold simple-match-card-team__score"
        )
        aux = [
            re.search(regex_rule, i.text)[0].replace("(", "").replace(")", "")
            for i in aux
        ]
        return aux

    def get_all_matches(self, html_full):
        aux_dict = []
        for match in html_full.find_all("a", class_="match-card", href=True):
            aux_dict_2 = {}
            aux_dict_2["link"] = "https://onefootball.com" + match["href"]
            aux_dict_2["stage"] = match.find_previous(
                "h3", class_="title-7-medium section-header__subtitle"
            ).text
            try:
                aux_dict_2["date"] = match.find("time").text
                aux_dict_2["pens"] = False
                aux_dict_2["pens_home_score"] = False
                aux_dict_2["pens_away_score"] = False
            except:
                try:
                    aux_dict_2["date"] = False
                    aux_dict_2["pens"] = (
                        match.find(
                            "span",
                            class_="title-8-medium simple-match-card__info-message simple-match-card__info-message--secondary",
                        ).text
                        == "(Pens)"
                    )
                    home, away = self.get_penalties(match)
                    aux_dict_2["pens_home_score"] = home
                    aux_dict_2["pens_away_score"] = away
                except:
                    aux_dict_2["date"] = False
                    aux_dict_2["pens"] = False
                    aux_dict_2["pens_home_score"] = False
                    aux_dict_2["pens_away_score"] = False
            aux_dict.append(aux_dict_2)
        return aux_dict

    def verify_date(self, match):
        time_const = [" Today ", " Yesterday ", " Tomorrow "]
        try:
            if match["date"] in time_const:
                if match["date"] == time_const[1]:  # Yesterday
                    return True
                else:
                    return False
            else:
                match_date = datetime.strptime(match["date"], " %d/%m/%Y ")
                today_date = datetime.today()
                if match_date > today_date:
                    return False
                else:
                    return True
        except:
            False

    def extraction(self, url):
        print("Getting all matches from competition at ", url)
        page = self.get_html_full(url)
        matches = self.get_all_matches(page)

        matches = list(filter(self.verify_date, matches))
        print("Total of", len(matches), "Match(es)")

        def merge_information(match, second_try=False):
            try:
                match_info = self.get_info_from_match(
                    *self.get_html_from_a_match(match["link"])
                )
            except:
                print("Error", match["link"])
                if not second_try:
                    match_info = merge_information(match, True)
                else:
                    match_info = {"error": True}
            return {**match, **match_info}

        print("Starting the data extraction per match")
        aux_list = Parallel(n_jobs=self.n_jobs, backend="threading", verbose=10)(
            delayed(merge_information)(match) for match in matches
        )

        df = pd.DataFrame(aux_list)
        if "error" in df.columns:
            filter_error = df.error.isin([True])
            for i, row in df[filter_error].iterrows():
                row_aux = dict(row)
                print("Retrying errors", i, row_aux["link"])
                row_aux.pop("error")
                try:
                    aux_list[i] = merge_information(row_aux, True)
                except:
                    pass
        else:
            print("No errors found")

        df = pd.DataFrame(aux_list).drop("link", axis=1)
        df.reset_index(inplace=True)
        df.rename(columns={"index": "match_id"}, inplace=True)
        df["match_id"] = df["match_id"] + 1
        print("Final shape dataset", df.shape)
        return df

    def transform(self, df):
        print("Transforming data")
        df.date = df.date.replace(" Today ", datetime.today().strftime("%d/%m/%Y"))
        df.date = df.date.replace(
            " Yesterday ",
            (datetime.today() - timedelta(days=1)).strftime("%d/%m/%Y"),
        )
        df.date = df.date.replace(
            " Tomorrow ",
            (datetime.today() + timedelta(days=1)).strftime("%d/%m/%Y"),
        )
        # Prediction
        df.prediction_quantity = df.prediction_quantity.str.replace(",", "")

        # Removing spaces
        for col in ["stage", "date", "team_name_home", "team_name_away", "location"]:
            df[col] = df[col].str.strip()
        df[["date", "prediction_quantity"]].head()
        columns = [
            "possession_home",
            "possession_away",
            "duels_won_home",
            "duels_won_away",
            "prediction_team_home_win",
            "prediction_draw",
            "prediction_team_away_win",
        ]
        print("Cleaning percentage Columns")

        def clean_percentage_columns(df, columns_list):
            for column in columns_list:
                if column in df.columns:
                    df[column] = df[column].astype(str)
                    df[column] = df[column].str.replace("%", "")
                    df[column] = df[column].astype(float)
                    df[column] = df[column] / 100
            return df

        clean_percentage_columns(df, columns)
        # transforming lineup_home list of dictionaries into new columns: player_names_home and player_numbers_home
        print("Extract players data")

        def extract_player_data(lineup):
            if not lineup or not isinstance(lineup, list):
                return None, None
            player_names = [
                player["player_name"] for player in lineup if isinstance(player, dict)
            ]
            player_numbers = [
                player["player_number"] for player in lineup if isinstance(player, dict)
            ]
            if len(player_names) != len(player_numbers):
                print(lineup)
                return None, None
            return (player_names, player_numbers)

        df["player_names_home"] = df.apply(
            lambda row: extract_player_data(row["lineup_home"])[0], axis=1
        )
        df["player_numbers_home"] = df.apply(
            lambda row: extract_player_data(row["lineup_home"])[1], axis=1
        )
        df["player_names_away"] = df.apply(
            lambda row: extract_player_data(row["lineup_away"])[0], axis=1
        )
        df["player_numbers_away"] = df.apply(
            lambda row: extract_player_data(row["lineup_away"])[1], axis=1
        )
        df["player_names_home"] = df["player_names_home"].apply(
            lambda x: list(map(str.strip, x))
        )
        df["player_numbers_home"] = df["player_numbers_home"].apply(
            lambda x: list(map(str.strip, x))
        )
        df["player_names_away"] = df["player_names_away"].apply(
            lambda x: list(map(str.strip, x))
        )
        df["player_numbers_away"] = df["player_numbers_away"].apply(
            lambda x: list(map(str.strip, x))
        )
        df_events_list = pd.DataFrame(
            columns=[
                "match_id",
                "team",
                "event_team",
                "event_time",
                "event_type",
                "action_player_1",
                "action_player_2",
            ]
        )

        for i, row in df.iterrows():
            events_list = row["events_list"]
            match_id = row.match_id
            for event in events_list:
                if event.get("event_team") == "home":
                    team = row.team_name_home
                else:
                    team = row.team_name_away

                df_events_list = df_events_list.append(
                    {
                        "match_id": match_id,
                        "team": team,
                        "event_team": event.get("event_team"),
                        "event_time": event.get("event_time"),
                        "event_type": event.get("event_type"),
                        "action_player_1": event.get("action_player_1"),
                        "action_player_2": event.get("action_player_2"),
                    },
                    ignore_index=True,
                )

        df.drop("events_list", axis=1, inplace=True)
        print("Generating Event dataset")
        df_events_list.reset_index(inplace=True)
        df_events_list["event_time"] = df_events_list["event_time"].str.replace("'", "")
        df_events_list.rename(columns={"index": "event_id"}, inplace=True)
        df_events_list["event_id"] = df_events_list["event_id"] + 1
        df_events_list.head()

        return df, df_events_list

    def store(self, df_matches, df_event, dataset_name, dataset_folder):
        print("Saving dataset as", dataset_name)
        if not os.path.exists(dataset_folder):
            os.makedirs(dataset_folder)

        df_matches.to_csv(
            os.path.join(dataset_folder, f"matches_{dataset_name}.csv"), index=False
        )
        df_event.to_csv(
            os.path.join(dataset_folder, f"events_{dataset_name}.csv"), index=False
        )
