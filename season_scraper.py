


from br_web_scraper import client
import database as db
import datetime
from sqlalchemy import create_engine


def season_to_dict_list(season):
    """Takes a season, with the default br_web_scraper format, and converts it so that
    dict[key] = [] where the list contains all values in the season found with that key"""

    dict_list = dict()
    for game in season:
        keys = game.keys()
        for key in keys:
            if key not in dict_list.keys():
                dict_list[key] = []
            dict_list[key].append(game[key])
    return dict_list


def cd_class_to_string(season):
    """Takes a season, as returned by client.season_schedule(), extracts all the enums, and replaces them with their
    values"""

    new_season =[]
    for game in season:
        game_dict = dict()
        keys = game.keys()
        for key in keys:
            if type(game[key]) not in [str, int, float, datetime.datetime]:
                game_dict[key] = game[key].value
            else:
                game_dict[key] = game[key]
        new_season.append(game_dict)
    return new_season


db_url = "sqlite:///database//nba_db.db"
engine = create_engine(db_url)
year = 2019
tbl_name = "sched_{}".format(year)

# Create table
season = client.season_schedule(year)
sql_types = db.get_sql_type(season[0])  # Send first element of season list to get tbl formatting
col_defs = db.create_col_definitions(tbl_name, sql_types)
db.create_table(engine, tbl_name, col_defs, overwrite=True)

# Insert rows
season = cd_class_to_string(season)
tbl_db = db.get_table(engine, tbl_name)
db.insert_rows(engine, tbl_db, season)

print("FINISHED")