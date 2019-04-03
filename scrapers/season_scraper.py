"""
Author: Spencer Weston

Purpose: season_scraper scrapes data from a specified season and writes it to the specified database. The
basketball_reference_web_scraper package is used to scrape the data. The data is then formatted and written to the
database. The table is automatically named 'sched' for schedule with the year appended as in 'sched_2019'.

Args (default):
    year (2019): The year of the season desired
    db_url ('sqlite:///database//nba_db.db'): Path to the database where data should be written
"""

from datetime import datetime
import pandas
from sqlalchemy import UniqueConstraint


from br_web_scraper import client
from database.manipulator import DataManipulator


def season_to_dict_list(season):
    """ DELETE???

    Take a season, parse it into a dictionary of lists, and return the dictionary

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
            if type(game[key]) not in [str, int, float, datetime]:
                game_dict[key] = game[key].value  # Extract value from enum here
            else:
                game_dict[key] = game[key]
        new_season.append(game_dict)
    return new_season


def create_season_table(database, data, tbl_name):
    """Creates the season table in the specified database, inserts the data, and clears mappers"""
    sql_types = data.get_sql_type()
    constraint = {UniqueConstraint: ["start_time", "home_team", "away_team"]}
    database.map_table(tbl_name, sql_types, constraint)
    database.create_tables()
    # client.season_schedule() returns data in row form. The necessary formatting is done by br_enum_to_string().
    # data.data is passed, rather than season, just to be explicit and consistent with other scrapers
    database.insert_rows(tbl_name, data.data)
    database.clear_mappers()  # if mappers aren't cleared, others scripts won't be able to use template


def update_season_table(session, schedule, season_df):
    """Updates the schedule table in the database with new data stored in the season_df"""
    date = datetime.date(datetime.now())
    update_rows = session.query(schedule).filter(schedule.start_time < date,
                                                 schedule.home_team_score == 0).order_by(schedule.start_time)
    if update_rows.count() == 0:
        print("Season is up to date; Returning without performing an update.")
        return
    first_game_time = update_rows.all()[0].start_time
    last_game_time = update_rows.all()[len(update_rows.all()) - 1].start_time

    # Reduce season to games between first and last game time
    season_df["start_time"] = season_df["start_time"].dt.tz_localize(None)
    season_df = season_df.loc[(season_df.start_time >= first_game_time) & (season_df.start_time <= last_game_time)]

    for row in update_rows.all():
        game = season_df.loc[(season_df.home_team == row.home_team) & (season_df.away_team == row.away_team)]
        row.home_team_score = int(game.home_team_score)
        row.away_team_score = int(game.away_team_score)
        session.add(row)


def scrape(database, session, year=2019):
    """Scrape basketball reference for games in a season, parse the output, and write the output to a database.

    If the specified year has been completed, it will return every game in the season. If the season is ongoing, it will
    return every game up to the day before the module is run. This ensures only completed games are returned.

    Args:
        database: A Database class from database.py which dictates table interactions
        session: An instantiated session object from sqlalchemy
        year (2019): The league year of the desired season

    To-do:
        Add
    """
    tbl_name = "sched_{}".format(year)

    # Create table
    season_data = client.season_schedule(year)
    season_data = br_enum_to_string(season_data)
    data = DataManipulator(season_data)

    if not database.table_exists(tbl_name):  # Creates database
        create_season_table(database, data, tbl_name)

    else:  # Updates database
        schedule = database.get_table_mappings([tbl_name])
        update_season_table(session, schedule, pandas.DataFrame(season_data))

    return True
