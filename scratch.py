import requests
from lxml import html
from basketball_reference_web_scraper.errors import InvalidDate
from basketball_reference_web_scraper.parsers.box_scores import parse_player_box_scores
from basketball_reference_web_scraper.parsers.schedule import parse_schedule, parse_schedule_for_month_url_paths
from basketball_reference_web_scraper.parsers.players_season_totals import parse_players_season_totals
from bs4 import BeautifulSoup
import re

BASE_URL = 'https://www.basketball-reference.com'

def parse_player_box_scores_test(page):
    tree = html.fromstring(page)
    rows = tree.xpath('//table[@id="stats"]//tbody/tr[not(contains(@class, "thead"))]')
    return list(map(lambda row: parse_player_box_score_test(row), rows))


def xml_rows_to_dictionary(rows):
    return{
        "observation" : str(rows[1].text_content())
    }

def parse_player_box_score_test(row):
    return {
        "name": str(row[1].text_content()),
#        "team": TEAM_ABBREVIATIONS_TO_TEAM[row[2].text_content()],
#        "location": parse_location(row[3].text_content()),
#        "opponent": TEAM_ABBREVIATIONS_TO_TEAM[row[4].text_content()],
#        "outcome": parse_outcome(row[5].text_content()),
#        "seconds_played": int(parse_seconds_played(row[6].text_content())),
        "made_field_goals": int(row[7].text_content()),
        "attempted_field_goals": int(row[8].text_content()),
        "made_three_point_field_goals": int(row[10].text_content()),
        "attempted_three_point_field_goals": int(row[11].text_content()),
        "made_free_throws": int(row[13].text_content()),
        "attempted_free_throws": int(row[14].text_content()),
        "offensive_rebounds": int(row[16].text_content()),
        "defensive_rebounds": int(row[17].text_content()),
        "assists": int(row[19].text_content()),
        "steals": int(row[20].text_content()),
        "blocks": int(row[21].text_content()),
        "turnovers": int(row[22].text_content()),
        "personal_fouls": int(row[23].text_content()),
        "game_score": float(row[25].text_content()),
    }

#test = team_box_scores("2019", "misc_stats")
def team_box_scores(year, tbl_name):
    url = '{BASE_URL}/leagues/NBA_{year}.html'.format(
        BASE_URL=BASE_URL,
        year=year
    )

    response = requests.get(url=url, allow_redirects=False)

    if 200 <= response.status_code < 300:
        return parse_table(response.content, tbl_name)

    raise InvalidDate(year=year)

def parse_table(page, tbl_name):


    #comm = re.compile('<!--|-->')
    cleaned_soup = BeautifulSoup(re.sub("<!--|-->", "", str(page)), features="lxml")
    table = cleaned_soup.find('table', {'id': '{}'.format(tbl_name)})
    data_dict = get_data_dict_from_tbl(table)

    return(data_dict)

def get_data_from_tbl(table, remove_nones=True):
    rows = table.find_all("tr")
    data = [[td.findChildren(text=True) for td in tr.findAll("td")] for tr in rows]
    if remove_nones:
        data = [x for x in data if x]
    return(data)

def no_class(tag):
    return not tag.has_attr('class')

data_stat_headers = [
    "team_name", "age", "wins",
    "losses", "wins_pyth", "losses_pyth",
    "mov", "sos", "srs", "off_rtg",
    "def_rtg", "pace", "fta_per_fga_pct",
    "fg3a_per_fga_pct", "ts_pct",
    "efg_pct", "tov_pct", "orb_pct",
    "ft_rate", "opp_efg_pct", "opp_tov_pct",
    "drb_pct", "opp_ft_rate", "arena_name",
    "attendance", "attendance_per_g",
]

def get_data_dict_from_tbl(table):
    rows = table.find_all("tr")
    data_dict = dict()
    for row in rows:
        if (row.find('th', {"scope": "row"}) != None):
            for head in data_stat_headers:
                cell = row.find("td", {"data-stat": head})
                a = cell.text.strip().encode()
                cell_data = a.decode("utf-8")
                #cell_header= cell.get("data-stat")
                if head in data_dict:
                    data_dict[head].append(cell_data)
                else:
                    data_dict[head] = [cell_data]
    return data_dict

#'date_game'}  # add more features here!!
#rows = table.find_all('tr')
#for row in rows:
#    if
#(row.find('th', {"scope": "row"}) != None):
#for f in featuresWanted:
#    cell = row.find("td", {"data-stat": f})
#a = cell.text.strip().encode()
#text = a.decode("utf-8")
#if f in pre_df:
#    pre_df[f].append(text)
#else:
#    pre_df[f] = [text]


def misc_stats_tbl_row_to_dict(data):
    return{
        "team" : str(data[0][0])

    }

#HTML scrape test
year = 2019
test = team_box_scores(2019, "misc_stats")

print(test)