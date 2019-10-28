"""
team_scraper scrapes and stores team stats from basketball reference.

By default, it scrapes miscellaneous stats from 2019. Alternate years and tables may be scraped though functionality is
not yet guaranteed. The scraped tables are written to the specified database.

ToDo:
    1. Create a method for stripping extraneous characters from team-names. If querying a historical season (<2001),
    the teams that made the playoffs have a '*' appended that we want to strip from the team-name
    2. Change so that only data is returned and no database operations are performed
    3. Change to include the scrape time for the data
"""

from bs4 import BeautifulSoup  # Requires lxml to be installed as well
from datetime import datetime
import re
import requests

# Local imports.
from nbapredict.configuration import Config
from nbapredict.helpers.br_references import BASE_URL
from nbapredict.helpers.br_references import data_stat_headers as headers
from nbapredict.helpers import type


def team_statistics(tbl_name):
    """Build a URL for the specified year and return team statistics for the specified table on that page.

    Performance not guaranteed for tables that are not "misc_stats"

    Args:
        tbl_name: The name of the table to be returned

    Returns:
        A dictionary version of the specified table. Keys are column titles that return lists ordered by team.
    """

    url = '{BASE_URL}/leagues/NBA_{year}.html'.format(
        BASE_URL=BASE_URL,  # imported from br_references.py
        year=Config.get_property("league_year")
    )

    response = requests.get(url=url, allow_redirects=False)
    if 200 <= response.status_code < 300:
        scrape_time = datetime.now()
        return parse_table(response.content, tbl_name, scrape_time)  # Note that this uses the .content attribute

    raise Exception("Could not connect to URL")


def parse_table(page, tbl_name, scrape_time):
    """Parse the specified table on the specified page and return the data as a dictionary

     Args:
         page: The contents from a url response
         tbl_name: the desired table to be parsed

     Returns:
         A dictionary version of the specified table. Keys are column titles that return lists ordered by team.
     """

    cleaned_soup = BeautifulSoup(re.sub('<!--|-->', "", str(page)), features="lxml")  # Strips comments from page
    table = cleaned_soup.find('table', {'id': '{}'.format(tbl_name)})
    data_dict = get_data_dict_from_tbl(table)
    keys = data_dict.keys()
    for key in keys:
        data_dict[key] = type.set_type(data_dict[key])
    # Add a scrape time for each row in the dictionary
    data_dict['scrape_time'] = [scrape_time for i in range(len(data_dict[key]))]
    return data_dict


def get_data_dict_from_tbl(table):
    """Return a dictionary from a BeautifulSoup table with column names as keys and a list of values

    Args:
        table: a table as returned by the find method on a BeautifulSoup object
    """
    rows = table.find_all("tr")
    data_dict = dict()

    for row in rows:
        if row.find('th', {"scope": "row"}) is not None:
            for head in headers:
                cell = row.find("td", {"data-stat": head})
                a = cell.text.strip().encode()
                cell_data = a.decode("utf-8")

                if head in data_dict:
                    data_dict[head].append(cell_data)
                else:
                    data_dict[head] = [cell_data]

    return data_dict


def clean_team_name(team_names):
    """Take a list of team_names, modify the names to match the format specified in br_references, and return a new list

    Args:
        team_names: a list of team_names to be checked for validity, and if needed, modified
    """
    new_team_names = []
    for team in team_names:
        new_team_names.append(''.join(a for a in team if a.isalpha() or a.isspace() or a.isdigit()).upper())
    return new_team_names


def scrape(tbl_name="misc_stats"):
    """Scrape a basketball_reference table of team stats, parse the table, and write it to a database

    Args:
        database: An instantiated DBInterface object from database.database for database interactions
        tbl_name: The name of the table to scrape on basketballreference.com
    """

    # Get tbl_dictionary from basketball reference
    tbl_dict = team_statistics(tbl_name)
    tbl_dict["team_name"] = clean_team_name(tbl_dict["team_name"])
    return tbl_dict


if __name__ == "__main__":
    from nbapredict.database.dbinterface import DBInterface
    db = DBInterface()
    scrape()
