from classification_dicts import data_stat_headers as HEADERS
from classification_dicts import BASE_URL
from bs4 import BeautifulSoup
import re
import requests
import general
import database as db
from sqlalchemy import create_engine
from sqlalchemy import Table
from sqlalchemy import MetaData
from sqlalchemy import inspect


# USABLE STUFF
def team_box_scores(year, tbl_name):
    """Function builds a URL for the specified year and returns team box scores for a specified table on 
    that page"""

    url = '{BASE_URL}/leagues/NBA_{year}.html'.format(
        BASE_URL=BASE_URL,
        year=year
    )

    response = requests.get(url=url, allow_redirects=False)

    if 200 <= response.status_code < 300:
        return parse_table(response.content, tbl_name)  # Note that this uses the .content attribute

    print("Could not connect to URL")


def parse_table(page, tbl_name):
    """Takes the content from a URL response as page, uses re.sub to remove comments on the page, and finally
     returns a dictionary with the data from the specified table"""

    cleaned_soup = BeautifulSoup(re.sub('<!--|-->', "", str(page)), features="lxml")
    table = cleaned_soup.find('table', {'id': '{}'.format(tbl_name)})
    data_dict = get_data_dict_from_tbl(table)
    keys = data_dict.keys()
    for key in keys:
        data_dict[key] = general.set_type(data_dict[key])

    return data_dict


def get_data_from_tbl(table, remove_nones=True):
    """(ARCHIVE?) Returns only the data from a table"""
    rows = table.find_all("tr")
    data = [[td.findChildren(text=True) for td in tr.findAll("td")] for tr in rows]
    if remove_nones:
        data = [x for x in data if x]
    return data


def get_data_dict_from_tbl(table):
    """Returns a dictionary from a BeautifulSoup table with colnames as keys and a list of values"""

    rows = table.find_all("tr")
    data_dict = dict()

    for row in rows:
        if (row.find('th', {"scope": "row"}) != None):
            for head in HEADERS:
                cell = row.find("td", {"data-stat": head})
                a = cell.text.strip().encode()
                cell_data = a.decode("utf-8")

                if head in data_dict:
                    data_dict[head].append(cell_data)
                else:
                    data_dict[head] = [cell_data]

    return data_dict


# Pathes and URLs
year = 2019
tbl_name = "misc_stats"

# Get tbl_dictionary from basketball reference
tbl_dict = team_box_scores(year, tbl_name)

# Database work set_up
db_url = "sqlite:///database//nba_db.db"
engine = create_engine(db_url)
m = MetaData()

# Transform data into sql_alchemy format and write table to DB
sql_types = db.get_sql_type(tbl_dict)
col_defs = db.create_col_definitions(tbl_name, sql_types)
db.create_table(engine, tbl_name, col_defs, overwrite=False)

# Write rows to DB
if general.check_dict_list_equivalence(tbl_dict):
    tbl_db = db.get_table(engine, tbl_name)
    rows = db.dict_to_rows(tbl_dict)
    db.insert_rows(engine, tbl_db, rows)
else:
    print("tbl_dict rows are not equivalent length")


print("FINISHED")

#TO-DO:
# 3. Write to DB