"""
Author: Spencer Weston

Purpose: line_scraper is used to scrape betting odds from Bovada.

Args (defaults):

To-Do:
"""

import requests

# Local Imports
import database


def odds_for_today():
    """Build a URL for the specified year and return team box scores for a specified table on that page.

    Args:

    Returns:
    """

    url = "https://www.bovada.lv/services/sports/event/v2/events/A/description/basketball/nba"

    response = requests.get(url=url, allow_redirects=False).json()

    if 200 <= response.status_code < 300:
        print("here")

    raise Exception("Response status code = {}".format(response.status_code))

    # Move down tree towards games
    level_1 = response[0]
    level_2 = level_1["events"]  # events hold matchups
    level_3 = level_2[0]  # This is an individual game
    competitors = level_3["competitors"]
    level_4 = level_3["displayGroups"]  # Displayed betting information
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




def scrape(database, year=2019):
    """Macro level function to scrape betting lines from Bovada.

    Args:
        database: A database class from database.py
        year: The desired league year to scrape. In all likelihood, this will always be the current league year as
        Bovada, the scraped site, displays only day-of our future date betting lines.
    """

    schedule = database.get_tables("sched_{}".format(year))


    print("something")


if __name__ == "__main__":
    db = database.Database(r"sqlite:///../database//nba_db.db")
    year = 2019
    scrape(db, year)



