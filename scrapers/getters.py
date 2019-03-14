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


def get_spread_for_games(odds_table, session, games):
    """Return the spreads from the odds_table that correspond to the games

    Args:
        odds_table: Sqlalchemy table object that contains odds
        session: Sqlalchemy session object
        games: Pandas dataframe of the games to extract spreads for
    """
    for game, odds in session.query(games, odds_table).filter(odds_table.away_team == games.away_team).all():
        print(game)
        print(odds)
    test = session.query(odds_table).filter(odds_table.home_team.in_(games.home_team),
                                            odds_table.away_team.in_(games.away_team))

    test = 2