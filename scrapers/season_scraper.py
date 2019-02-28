"""
Author: Spencer Weston

Purpose: season_scraper scrapes data from a specified season and writes it to the specified database. The
basketball_reference_web_scraper package is used to scrape the data. The data is then formatted and written to the
database. The table is automatically named 'sched' for schedule with the year appended as in 'sched_2019'.

Args (default):
    year (2019): The year of the season desired
    db_url ('sqlite:///database//nba_db.db'): Path to the database where data should be written
"""


from br_web_scraper import client
import database as db
import datetime
import os
from sqlalchemy import create_engine


def season_to_dict_list(season):
    """Take a season, parse it into a dictionary of lists, and return the dictionary

    Args:
        season: A season formatted as if returned by basketball_reference_web_scraper

    Returns:
        A dictionary formatted as dict[key] = [] where the list contains all values in the season found for that key.
        Each list is ordered so that all information from game 1 will be in index 0, game 2 in index 1, and so on.
        For example,

        dict['start_time] = [time1,time2,time3]
        dict['home_team] = [h_team1, h_team2, h_team3]
        dict['away_team] = [a_team1, a_team2, a_team3]

        with other keys, such as 'home_team_score' included.
    """
    dict_list = dict()
    for game in season:
        keys = game.keys()
        for key in keys:
            if key not in dict_list.keys():
                dict_list[key] = []
            dict_list[key].append(game[key])
    return dict_list


def br_enum_to_string(season):
    """Substitute the value of each enum into the enum's position in season and return a modified season

    Args:
        season: A season as defined by basketball_reference_web_scraper

    Returns:
        A season modified so that any enums in the season are replaced by their values
    """
    new_season = []
    for game in season:
        game_dict = dict()
        keys = game.keys()
        for key in keys:
            if type(game[key]) not in [str, int, float, datetime.datetime]:
                game_dict[key] = game[key].value  # Extract value from enum here
            else:
                game_dict[key] = game[key]
        new_season.append(game_dict)
    return new_season


def main(session, database, year=2019, db_url="sqlite:///database//nba_db.db"):
    """Scrape basketball reference for games in a season, parse the output, and write the output to a database.

    If the specified year has been completed, it will return every game in the season. If the season is ongoing, it will
    return every game up to the day before the module is run. This ensures only completed games are returned.

    Args:
    year (2019): The year of the season desired
    db_url ('sqlite:///database//nba_db.db'): Path to the database where data should be written
    """

    engine = create_engine(db_url)
    tbl_name = "sched_{}".format(year)

    # Create table
    season = client.season_schedule(year)
    sql_types = db.get_sql_type(season[0])
    database.map_table(tbl_name, sql_types)
    database.create_table()
    db.clear_mappers()  # if mappers aren't cleared, others scripts won't be able to use template
    session.close()
    return True
    # Send first element of season list to get tbl formatting for a row
    #col_defs = db.create_col_definitions(tbl_name, sql_types)
    #db.create_table(engine, tbl_name, col_defs, overwrite=True)

    # Insert rows
    season = br_enum_to_string(season)
    tbl_db = db.get_table(engine, tbl_name)
    db.insert_rows(engine, tbl_db, season)

    print("FINISHED\n")


if __name__ == "__main__":
    main()

