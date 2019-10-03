"""
season_scraper scrapes data from a specified season and writes it to the specified database.

The basketball_reference_web_scraper package is used to scrape the data. The data is then formatted and written to the
database. The table is automatically named 'sched' for schedule with the year appended as in 'sched_2019'.
"""

from datetime import datetime
import pandas
from sqlalchemy import UniqueConstraint, func

# Local Imports
from nbapredict.br_web_scraper import client
from nbapredict.database.manipulator import DataManipulator
from nbapredict.configuration import Config


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
        database: An instantiated DBInterface object from database.database for database interactions.
        data: A DataManipulator object from database.manipulator that holds the data to add.
        tbl_name: The name of the table to create.
    """
    sql_types = data.get_sql_type()
    constraint = {UniqueConstraint: ["start_time", "home_team", "away_team"]}
    database.map_table(tbl_name, sql_types, constraint)
    database.create_tables()
    database.insert_rows(tbl_name, data.data)
    database.clear_mappers()  # if mappers aren't cleared, others scripts won't be able to use DBInterface.Template


def update_season_table(session, sched_tbl, season_df):
    """Updates the schedule table in the database with new data stored in the season_df

    Changes are added to the session and need to be committed later.
    During the playoffs, some games are removed from the sched_df as described in line.

    Args:
        session: A SQLalchemy session object
        sched_tbl: A mapped table that holds the schedule
        season_df: A pandas Dataframe version of the season as returned from br_web_scraper
    """
    date = datetime.date(datetime.now())
    update_rows = session.query(sched_tbl).filter(sched_tbl.start_time < date,
                                                  sched_tbl.home_team_score == 0).order_by(sched_tbl.start_time)
    if update_rows.count() == 0:
        # print("Season is up to date; Returning without performing an update.") Test/logging statement
        return

    all_update_rows = update_rows.all()
    first_game_time = all_update_rows[0].start_time
    last_game_time = all_update_rows[len(all_update_rows) - 1].start_time

    # Reduce season to games between first and last game time
    season_df["start_time"] = season_df["start_time"].dt.tz_localize(None)
    update_df = season_df.loc[(season_df.start_time >= first_game_time) & (season_df.start_time <= last_game_time)]

    for row in all_update_rows:
        game = update_df.loc[(update_df.home_team == row.home_team) & (update_df.away_team == row.away_team) &
                             (update_df.start_time.dt.date == datetime.date(row.start_time))]
        if len(game) == 0:
            # This catches playoff games which do not end up happening (i.e. a game 7 in a series a team sweeps), and
            # removes it from the database
            session.delete(row)
        else:
            row.home_team_score = int(game.home_team_score)
            row.away_team_score = int(game.away_team_score)
            row.start_time = game.start_time.dt.to_pydatetime()[0]  # Convert Pandas TimeStamp to datetime
            session.add(row)


def add_rows(session, schedule, rows):
    """Add rows into the schedule if they contain games past the most recent game in schedule.

    Args:
        session: An instantiated sqlalchemy session
        schedule: A mapped schedule table
        rows: rows compatible with schedule
    """
    most_recent_game = session.query(func.max(schedule.start_time)).one()[0]  # The most recent game in the database
    most_recent_game = most_recent_game.replace(tzinfo=rows[0]["start_time"].tzinfo)  # Unify timezones
    new_rows = [row for row in rows if row["start_time"] > most_recent_game]
    new_row_objects = []
    for row in new_rows:
        new_row_objects.append(schedule(**row))
    session.add_all(new_row_objects)


def scrape(database, session):
    """Scrape basketball reference for games in a season, parse the output, and write the output to a database.

    If the specified year has been completed, it will return every game in the season. If the season is ongoing, it will
    return every game up to the day before the module is run. This ensures only completed games are returned.

    Args:
        database: An instantiated DBInterface object from database.database for database interactions
        session: A SQLalchemy session object
    """
    league_year = Config.get_property("league_year")
    tbl_name = "sched_{}".format(league_year)

    # Create table
    season_data = client.season_schedule(league_year)
    season_data = br_enum_to_string(season_data)
    data = DataManipulator(season_data)

    if not database.table_exists(tbl_name):  # Creates database
        create_season_table(database, data, tbl_name)

    else:  # Updates database
        schedule = database.get_table_mappings([tbl_name])
        update_season_table(session, schedule, pandas.DataFrame(season_data))  # Update rows with new data
        if len(data.data) > session.query(schedule).count():
            add_rows(session, schedule, data.data)  # Add new rows not in database

    return True


if __name__ == '__main__':
    from nbapredict.database.dbinterface import DBInterface
    from sqlalchemy.orm import Session
    db = DBInterface()
    session = Session(db.engine)
    scrape(db, session)
