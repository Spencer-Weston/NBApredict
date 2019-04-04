from datetime import timedelta
import pandas as pd


def get_games_on_day(schedule, session, date):
    """Return the games from schedule on the specified date

    Args:
        schedule: A mapped table object containing a schedule of games
        session: An instantiated session object
        date: The date to check for games
    """
    next_day = date + timedelta(days=1)
    return session.query(schedule).filter(schedule.start_time > date, schedule.start_time < next_day)


def get_first_game_time_on_day(schedule, session, date):
    games_on_day = get_games_on_day(schedule, session, date).subquery()
    first_game = session.query(games_on_day).order_by(games_on_day.c.start_time).first()
    if first_game:
        first_game_time = first_game[1]
        return first_game_time
    else:
        return None

def get_spreads_for_date(odds_table, session, date):
    """Return the spreads from the odds_table that correspond to the games

    Args:
        odds_table: Sqlalchemy table object that contains odds
        session: Sqlalchemy session object
        date: Date to extract odds for
    """
    next_day = date + timedelta(days=1)
    query = session.query(odds_table.start_time, odds_table.home_team, odds_table.away_team, odds_table.spread). \
                filter(odds_table.start_time > date, odds_table.start_time < next_day)

    return query


def get_pandas_df_from_table(database, session, tbl_name, qualifiers=False):
    """Convert the specified table into a pandas dataframe, modify it according to qualifiers, and return the result

    Args:
        database: An instantiated Database class from database.py
        session: SQLalchemy session object
        tbl_name: name of the desired table
        qualifiers: A list of columns or a function to filter rows by
        """
    tbl = database.get_table_mappings(tbl_name)
    query = session.query(tbl)
    if qualifiers:
        return pd.read_sql(query.statement, query.session.bind)[qualifiers]
    else:
        return pd.read_sql(query.statement, query.session.bind)
