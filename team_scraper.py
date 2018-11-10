import classification_dicts as cd
from classification_dicts import data_stat_headers as HEADERS
from classification_dicts import BASE_URL
import pandas
import numpy as np
import csv
from bs4 import BeautifulSoup
import re
import requests


def write_table_txt(table, out_file, write_type):
    """Writes the contents of table to the specified out_file"""
    file = open(out_file, write_type)
    file.write(table.text)
    print("table written to" + out_file)

def write_table_csv(table, out_file, write_type):
    pass


### USABLE STUFF
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

    raise InvalidDate(year=year)


def parse_table(page, tbl_name):
    """Takes the content from a URL response as page, uses re.sub to remove comments on the page, and finally
     returns a dictionary with the data from the specified table"""

    # comm = re.compile('<!--|-->')
    cleaned_soup = BeautifulSoup(re.sub("<!--|-->", "", str(page)), features="lxml")
    table = cleaned_soup.find('table', {'id': '{}'.format(tbl_name)})
    data_dict = get_data_dict_from_tbl(table)

    return (data_dict)


def get_data_from_tbl(table, remove_nones=True):
    """(ARCHIVE?) Returns only the data from a table"""
    rows = table.find_all("tr")
    data = [[td.findChildren(text=True) for td in tr.findAll("td")] for tr in rows]
    if remove_nones:
        data = [x for x in data if x]
    return (data)


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
txt_out = r"./test.txt"
csv_out = r"./test.csv"
year = 2019
tbl = "misc_stats"

tbl_dict = team_box_scores(year, tbl)
print("FINISHED")

#TO-DO:
# 1. Convert table_dict to pandas DF
# 2. Write to CSV
# 3. Write to DB 