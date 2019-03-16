from datetime import datetime, timedelta


def get_games_on_day(schedule, session, date):
    """Return the games from schedule on the specified date

    Args:
        schedule: A mapped table object containing a schedule of games
        session: An instantiated session object
        date: The date to check for games
    """
    next_day = date + timedelta(days=1)
    return session.query(schedule).filter(schedule.start_time > date, schedule.start_time < next_day)


def get_spreads_for_date(odds_table, session, date):
    """Return the spreads from the odds_table that correspond to the games

    Args:
        odds_table: Sqlalchemy table object that contains odds
        session: Sqlalchemy session object
        date: Date to extract odds for
    """
    next_day = date + timedelta(days=1)
    query = session.query(odds_table.start_time, odds_table.home_team, odds_table.away_team, odds_table.spread).\
                    filter(odds_table.start_time > date, odds_table.start_time < next_day)

    return query
