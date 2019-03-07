"""
Author: Spencer Weston

Purpose: line_scraper is used to scrape betting odds from Bovada.

Args (defaults):

To-Do:
"""

from datetime import datetime, timedelta
import requests
import pandas


# Local Imports


def odds_for_today(games_df):
    """Build a URL for the specified year and return team box scores for a specified table on that page.

    Args:
        games_df: A pandas dataframe of games which holds games on a certain date (Should be the current date to reflect
        the current games on Bovada

    Returns:
    """

    url = "https://www.bovada.lv/services/sports/event/v2/events/A/description/basketball/nba"

    response = requests.get(url=url, allow_redirects=False).json()

    # Move down tree towards games
    level_1 = response[0]
    level_2 = level_1["events"]

    game_descriptions = ["{} @ {}".format(home_tm, away_tm) for ]
    bovada_games = [game_dict for game_dict in level_2 if "@" in game_dict["description"]]

    # events hold matchups
    level_3 = level_2[0]  # This is an individual game
    competitors = level_3["competitors"]
    level_4 = level_3["displayGroups"]  # All Displayed betting information (Full game + other bets)
    level_5 = level_4[0]  # Betting information specific to the full game
    level_6 = level_5["markets"]  # Container that holds betting info but also a lot of random stuff
    level_7 = level_6[0]  # Further specifies full match lines
    level_8 = level_7["outcomes"]  # A list containing dictionaries with the spread for each team

    team_1 = level_8[0]
    team_2 = level_8[1]

    # Example run through of team_1
    team_name = team_1["description"]
    home_or_away = team_1["type"]  # Type 'A' is away; Type 'H' is home
    team_price = team_1["price"]  # Dictionary that holds the team handicap and odds (in many forms)
    team_spread = float(team_price["handicap"])


def get_games_on_day(schedule, session, date):
    """Return the games from schedule on the specified date

    Args:
        schedule: A mapped table object containing a schedule of games
        session: An instantiated session object
        date: The date to check for games
    """
    next_day = date + timedelta(days=1)
    return session.query(schedule).filter(schedule.c["start_time"] > date, schedule.c["start_time"] < next_day).\
        order_by(schedule.c["start_time"])


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
    games = get_games_on_day(schedule, session, date)
    games_df = pandas.DataFrame(games)

    odds_for_today(games_df)
    print("something")


# if __name__ == "__main__":
    # db = database.Database(r"sqlite:///../database//nba_db.db")
    # year = 2019
    # scrape(db, year)



