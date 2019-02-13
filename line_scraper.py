"""
Author: Spencer Weston

Purpose: line_scraper is used to scrape betting odds from Bovada.

Args (defaults):

To-Do:
"""

from bs4 import BeautifulSoup
import re
import requests
from sqlalchemy import create_engine


def odds_for_today():
    """Build a URL for the specified year and return team box scores for a specified table on that page.

    Args:

    Returns:
    """

    url = "https://www.bovada.lv/services/sports/event/v2/events/A/description/basketball/nba"

    response = requests.get(url=url, allow_redirects=False)

    if 200 <= response.status_code < 300:
        print("here")
        parse_odds(response.content)

    raise Exception("Response status code = {}".format(response.status_code))


def parse_odds(page):
    cleaned_soup = BeautifulSoup(re.sub('<!--|-->', "", str(page)), features="lxml")
    table = cleaned_soup.find('span', {'class': 'market-line bet-handicap'})
    test=0

if __name__ == "__main__":
    odds_for_today()



