"""
season_scraper scrapes data from a specified season and writes it to the specified database.

The basketball_reference_web_scraper package is used to scrape the data. The data is then formatted and written to the
database. The table is automatically named 'sched' for schedule with the year appended as in 'sched_2019'.
"""

from datetime import datetime
import pandas
from sqlalchemy import UniqueConstraint

# Local Imports
from br_web_scraper import client
from database.manipulator import DataManipulator


def br_enum_to_string(season):
    """Substitute the value of each enum for an enum in season and return a modified season

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
    """Creates the season table in the specified database, inserts the data, and clears mappers

    Use only if the table does not already exist

    Args:
        database: An instantiated Database object from database.database for database interactions.
        data: A DataManipulator object from database.manipulator that holds the data to add.
        tbl_name: The name of the table to create.
    """
    sql_types = data.get_sql_type()
    constraint = {UniqueConstraint: ["start_time", "home_team", "away_team"]}
    database.map_table(tbl_name, sql_types, constraint)
    database.create_tables()
    database.insert_rows(tbl_name, data.data)
    database.clear_mappers()  # if mappers aren't cleared, others scripts won't be able to use template


def update_season_table(session, sched_tbl, season_df):
    """Updates the schedule table in the database with new data stored in the season_df

    Changes are added to the session and need to be committed later.

    Args:
        session: A SQLalchemy session object
        sched_tbl: A mapped (i.e. queryable) table that holds the schedule
        season_df: A pandas Dataframe version of the season as returned from br_web_scraper
    """
    date = datetime.date(datetime.now())
    update_rows = session.query(sched_tbl).filter(sched_tbl.start_time < date,
                                                  sched_tbl.home_team_score == 0).order_by(sched_tbl.start_time)
    if update_rows.count() == 0:
        print("Season is up to date; Returning without performing an update.")
        return
    all_update_rows = update_rows.all()
    first_game_time = all_update_rows[0].start_time
    last_game_time = all_update_rows[len(update_rows.all()) - 1].start_time

    # Reduce season to games between first and last game time
    season_df["start_time"] = season_df["start_time"].dt.tz_localize(None)
    season_df = season_df.loc[(season_df.start_time >= first_game_time) & (season_df.start_time <= last_game_time)]

    for row in all_update_rows:
        game = season_df.loc[(season_df.home_team == row.home_team) & (season_df.away_team == row.away_team)]
        row.home_team_score = int(game.home_team_score)
        row.away_team_score = int(game.away_team_score)
        session.add(row)


def scrape(database, session, league_year=2019):
    """Scrape basketball reference for games in a season, parse the output, and write the output to a database.

    If the specified year has been completed, it will return every game in the season. If the season is ongoing, it will
    return every game up to the day before the module is run. This ensures only completed games are returned.

    Args:
        database: An instantiated Database object from database.database for database interactions
        session: A SQLalchemy session object
        league_year (2019): The league year of the desired season
    """
    tbl_name = "sched_{}".format(league_year)

    # Create table
    season_data = client.season_schedule(league_year)
    season_data = br_enum_to_string(season_data)
    data = DataManipulator(season_data)

    if not database.table_exists(tbl_name):  # Creates database
        create_season_table(database, data, tbl_name)

    else:  # Updates database
        schedule = database.get_table_mappings([tbl_name])
        update_season_table(session, schedule, pandas.DataFrame(season_data))

    return True
