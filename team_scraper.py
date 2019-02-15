"""
Author: Spencer Weston

Purpose: team_scraper is used to scrape team statistics from basketball reference. By default, it scrapes miscellaneous
statistics in 2019. Alternate years and tables may be scraped though functionality is not yet guaranteed. The scraped
tables are written to the specified database.

Args (defaults):
    Year (2019) - Desired year
    tbl_name ('misc_stats') - Name of the table to be scraped
    db_url ('sqlite:///database//nba_db.db') - Path to the database the table should be written to

To-Do:
    1. Create a method for stripping extraneous characters from team-names. If querying a historical season, the teams
    that made the playoffs have a '*' appended that we want to strip from the team-name
"""

from bs4 import BeautifulSoup  # Requires lxml to be installed as well
import re
import requests
import sqlite3
from sqlalchemy import create_engine
from sqlalchemy import exc
import os

# Local imports
from br_references import BASE_URL
from br_references import data_stat_headers as HEADERS
import database as db
import general


def team_statistics(year, tbl_name):
    """Build a URL for the specified year and return team box scores for a specified table on that page.

    Performance not guaranteed for tables that are not "misc_stats"

    Args:
        year: The year of the statistics to be returned
        tbl_name: The name of the table to be returned

    Returns:
        A dictionary version of the specified table. Keys are column titles that return lists ordered by team.
    """

    url = '{BASE_URL}/leagues/NBA_{year}.html'.format(
        BASE_URL=BASE_URL,  # imported from br_references.py
        year=year
    )

    response = requests.get(url=url, allow_redirects=False)

    if 200 <= response.status_code < 300:
        return parse_table(response.content, tbl_name)  # Note that this uses the .content attribute

    raise Exception("Could not connect to URL")


def parse_table(page, tbl_name):
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
        data_dict[key] = general.set_type(data_dict[key])

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
            for head in HEADERS:
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


def main(year=2019, tbl_name="misc_stats", db_url="sqlite:///database//nba_db.db"):
    """Scrape a basketball_reference table of team stats, parse the table, and write it to a database

    Args:
        year: Desired year
        tbl_name: Name of the table to be scraped
        db_url: Path to the database the table should be written to
    """
    if not os.path.isdir("database"):
        os.mkdir("database")

    # Get tbl_dictionary from basketball reference
    tbl_dict = team_statistics(year, tbl_name)
    tbl_dict["team_name"] = clean_team_name(tbl_dict["team_name"])

    # Database work set_up
    engine = create_engine(db_url)

    # Initial tbl_name is for scraping basketball reference; Year is added to disambiguate tables
    db_tbl_name = '{}_{}'.format(tbl_name, year)

    # Transform data into sql_alchemy format and write table to DB
    sql_types = db.get_sql_type(tbl_dict)
    col_defs = db.create_col_definitions(db_tbl_name, sql_types)

    # Initial thought was to create the database if we cannot create the table; However, it appears the create_table
    # function also creates the database if it doesn't exist. This relies on the folder being created beforehand as
    # handled at the start of main()
    db.create_table(engine, db_tbl_name, col_defs, overwrite=True)

    # Write rows to DB
    if general.check_dict_list_equivalence(tbl_dict):
        tbl_db = db.get_table(engine, db_tbl_name)
        rows = db.dict_to_rows(tbl_dict)
        db.insert_rows(engine, tbl_db, rows)
    else:
        # A possible scenario that we want to break on.
        raise Exception("tbl_dict rows are not equivalent length")

    print("FINISHED")


if __name__ == "__main__":
    main(year=2018)
