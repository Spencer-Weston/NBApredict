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



#HTML scrape test
year = 2019
test = team_box_scores(2019, "misc_stats")

print(test)