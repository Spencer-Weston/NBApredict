from classification_dicts import data_stat_headers as HEADERS
from classification_dicts import BASE_URL
from bs4 import BeautifulSoup
import re
import requests
import general
import db_interface as db_int
from sqlalchemy import create_engine
from sqlalchemy import Table
from sqlalchemy import MetaData
from sqlalchemy import inspect
from sqlalchemy import Column, Integer, String

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

    print("Could not connect to URL")


def parse_table(page, tbl_name):
    """Takes the content from a URL response as page, uses re.sub to remove comments on the page, and finally
     returns a dictionary with the data from the specified table"""

    # comm = re.compile('<!--|-->')
    cleaned_soup = BeautifulSoup(re.sub("<!--|-->", "", str(page)), features="lxml")
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
txt_out = r"./test.txt"
csv_out = r"./test.csv"
db = r"./database/nba_db.db"
year = 2019
tbl = "misc_stats"

tbl_dict = team_box_scores(year, tbl)
conn = db_int.db_connect(db)
#db_int.create_table_from_dict(conn, "misc_stats", tbl_dict, False)

db_uri = "sqlite:///database//nba_db.db"
engine = create_engine(db_uri)
m = MetaData()
table = Table('EX1', m,
              Column('id', Integer, primary_key=True),
              Column('key', String, nullable=True),
              Column('val', String))
#table.drop(engine)
#inspector = inspect(engine)
#print('EX1' in inspector.get_table_names())
engine.execute('CREATE TABLE "EX1" ('
               'id INTEGER NOT NULL,'
               'name VARCHAR, '
               'PRIMARY KEY (id));')

engine.execute('INSERT INTO "EX1"'
               '(id, name) '
               'VALUES (1, "raw1")')

result = engine.execute('SELECT * FROM '
                        '"EX1"')
for _r in result:
   print(_r)


print("FINISHED")

#TO-DO:
# 1. Convert table_dict to pandas DF
# 3. Write to DB