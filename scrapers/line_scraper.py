"""
Author: Spencer Weston

Purpose: line_scraper is used to scrape betting odds from Bovada.

Args (defaults):

To-Do:
"""

from datetime import datetime, timedelta
import requests
import pandas
from sqlalchemy import UniqueConstraint

# Local Imports
from database import DataManipulator
from scrapers import helpers


def odds_for_today(games_df):
    """Build a URL for the specified year and return team box scores for a specified table on that page.

    Args:
        games_df: A pandas dataframe of games which holds games on a certain date (Should be the current date to reflect
        the current games on Bovada

    Returns:
    """

    url = "https://www.bovada.lv/services/sports/event/v2/events/A/description/basketball/nba"

    response = requests.get(url=url, allow_redirects=False).json()
    scrape_time = datetime.now()

    # Move down tree towards games
    events = response[0]["events"]

    # Get the game dictionaries (which hold a bunch of random data) stripped from the events object
    game_descriptions = []
    for game in games_df.itertuples():
        game_descriptions.append("{} @ {}".format(game.away_team, game.home_team).lower())
    bovada_games = [game_dict for game_dict in events if game_dict["description"].lower() in game_descriptions]

    lines = {"home_team": [], "away_team": [], "start_time": [], "spread": [], "home_spread_price": [],
             "away_spread_price": [], "home_moneyline": [], "away_moneyline": [], "scrape_time": []}

    for game in bovada_games:
        home_team, away_team = parse_teams(game["competitors"])

        betting_info = game["displayGroups"][0]["markets"]
        full_match_bets = [bet for bet in betting_info if bet["period"]["description"] == "Match"]

        game_df = games_df.loc[(games_df.home_team == home_team.upper()) & (games_df.away_team == away_team.upper())]
        start_timestamp_df = game_df.start_time
        start_timestamp = start_timestamp_df.items().__next__()[1]
        start_datetime = start_timestamp.to_pydatetime()

        for bet in full_match_bets:
            if bet["description"] == "Moneyline":
                home_moneyline, away_moneyline = parse_moneyline(bet)
            elif bet["description"] == "Point Spread":
                spread, home_spread_price, away_spread_price = parse_spread(bet)
        game_lines = [home_team, away_team, start_datetime, spread, home_spread_price, away_spread_price,
                      home_moneyline, away_moneyline, scrape_time]

        # This section depends on python 3.7+ to preserve the order of dict keys in lines
        i = 0
        for key in lines:
            lines[key].append(game_lines[i])
            i += 1
    return lines


def parse_teams(competitors):
    """Parse a competitors object from Bovada and return the home and away teams, respectively"""
    if len(competitors) > 2:
        raise Exception("Unexpected objects in competitors")
    home_team = ""
    away_team = ""
    for team in competitors:
        if team["home"]:
            home_team = team["name"]
        else:
            away_team = team["name"]
    if not home_team == "" or away_team == "":
        return home_team.upper(), away_team.upper()
    else:
        raise Exception("Competitors was not properly parsed. Missing data.")


def parse_moneyline(moneyline_bet):
    """Parse a moneyline bet object from Bovada and return, in order, the home and away moneyline"""
    outcomes = moneyline_bet["outcomes"]
    home_moneyline = ""
    away_moneyline = ""
    if len(outcomes) > 2:
        raise Exception("Unexpected objects in moneyline bet")
    for o in outcomes:
        price = o["price"]["american"]
        if price == "EVEN":
            price = 100
        else:
            price = int(price)
        if o["type"] == "H":
            home_moneyline = price
        elif o["type"] == "A":
            away_moneyline = price
    if not home_moneyline == "" or away_moneyline == "":
        return home_moneyline, away_moneyline
    else:
        raise Exception("Moneyline was not properly parsed. Missing data.")


def parse_spread(spread_bet):
    """Parse a spread bet object from Bovada and return, in order, the spread and the home and away spread prices"""
    outcomes = spread_bet["outcomes"]
    spread = ""
    home_spread_price = ""
    away_spread_price = ""
    if len(outcomes) > 2:
        raise Exception("Unexpected objects in spread bet")
    for o in outcomes:
        if o["type"] == "H":
            spread = float(o["price"]["handicap"])
            home_spread_price = int(o["price"]["american"])
        elif o["type"] == "A":
            away_spread_price = int(o["price"]["american"])
    if not spread == "" or home_spread_price == "" or away_spread_price == "":
        return spread, home_spread_price, away_spread_price
    else:
        raise Exception("Spread was not properly parsed. Missing data.")


def create_odds_table(database, data, tbl_name):
    if not data.validate_data_length():
        raise Exception("Lengths in the data are not equal")
    sql_types = data.get_sql_type()
    constraint = {UniqueConstraint: ["home_team", "away_team", "start_time"]}
    database.map_table(tbl_name, sql_types, constraint)
    database.create_tables()

    rows = data.dict_to_rows()
    database.insert_rows(tbl_name, rows)
    database.clear_mappers()


def update_odds_table(odds_table, rows, session):
    row_objects = []
    for row in rows:
        row_object = odds_table(**row)
        row_objects.append(row_object)
    session.add_all(row_objects)


def scrape(database, session, year=2019):
    """Macro level function to scrape betting lines from Bovada.

    Args:
        database: A database class from database.py
        session: An instance of a sqlalchemy Session class bound to the database's engine
        year: The desired league year to scrape. In all likelihood, this will always be the current league year as
        Bovada, the scraped site, displays only day-of our future date betting lines.
    """

    schedule = database.get_tables("sched_{}".format(year))
    date = datetime.date(datetime.now())
    games = helpers.get_games_on_day(schedule, session, date)
    games_df = pandas.DataFrame(games)

    lines = odds_for_today(games_df)
    line_data = DataManipulator(lines)

    tbl_name = "odds_{}".format(year)
    tbl_exists = database.table_exists(tbl_name)
    if not tbl_exists:
        create_odds_table(database, line_data, tbl_name)

    elif line_data.validate_data_length() and tbl_exists:
        # All values in line_data are expected to be be unique from values in the database. A possible place for errors
        # to occur
        odds_table = database.get_table_mappings([tbl_name])
        update_odds_table(odds_table, line_data.dict_to_rows(), session)
    else:
        raise Exception("Somethings wrong here (Not descriptive, but this point shouldn't be hit.)")

    return True


# if __name__ == "__main__":
    # db = database.Database(r"sqlite:///../database//nba_db.db")
    # year = 2019
    # scrape(db, year)



