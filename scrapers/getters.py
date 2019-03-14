from datetime import datetime, timedelta


def get_games_on_day(schedule, session, date):
    """Return the games from schedule on the specified date

    Args:
        schedule: A mapped table object containing a schedule of games
        session: An instantiated session object
        date: The date to check for games
    To-Do:
        Rewrite in ORM format (See season_scraper)
    """
    next_day = date + timedelta(days=1)

    # Rough ORM format (Can't be cleanly manipulated by Pandas whereas the current version can use pd.DataFrame(games)
    # games = session.query(schedule).filter(schedule.start_time > date,
    #                                       schedule.start_time < next_day).order_by(schedule.start_time)

    return session.query(schedule).filter(schedule.c["start_time"] > date, schedule.c["start_time"] < next_day).\
        order_by(schedule.c["start_time"])


def get_spread_for_games(odds_table, session, games):
    """Return the spreads from the odds_table that correspond to the games

    Args:
        odds_table: Sqlalchemy table object that contains odds
        session: Sqlalchemy session object
        games: Pandas dataframe of the games to extract spreads for
    """
    test = session.query(odds_table).filter(odds_table.home_team.in_(games.home_team),
                                            odds_table.away_team.in_(games.away_team))

    test = 2