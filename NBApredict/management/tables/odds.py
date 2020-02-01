"""odds.py contains function to create the odds table in the database"""

import nbapredict.management.conversion as convert
from sqlalchemy import ForeignKey


def format_data(session, odds_dict, team_tbl, schedule_tbl):
    """From the odds_dict, strip extraneous dictionary keys, add a 'game_id' FK, and return the odds_dict

    Args:
        session: A SQLalchemy session bound to the db
        odds_dict: A dictionary of data returned by line_scraper
        team_tbl: A mapped team table
        schedule_tbl: A mapped schedule table

    Returns:
        odds_dict formatted with foreign keys (mainly a FK for games in the schedule tbl)
    """
    odds_dict['home_team_id'] = convert.values_to_foreign_key(session, team_tbl, "id", 'team_name',
                                                              odds_dict.pop('home_team'))
    # the columns that uniquely identify a game in the schedule table
    val_cols = ['home_team_id', 'start_time']
    uID = {k: odds_dict[k] for k in val_cols}  # Home team + start_time form a unique identifier for a game in schedule
    odds_dict['game_id'] = convert.values_to_foreign_key(session, schedule_tbl, "id", val_cols, uID)

    # Each of these columns is held in the schedule table
    del odds_dict['start_time']
    del odds_dict['away_team']
    del odds_dict['home_team_id']

    return odds_dict


def create_table(db, tbl_name, odds_data, schedule_tbl):
    """Create a table of odds in the database"""
    columns = odds_data.columns
    schedule_tbl_name = schedule_tbl.__table__.fullname
    columns['game_id'].append(ForeignKey("{}.id".format(schedule_tbl_name)))
    db.map_table(tbl_name=tbl_name, columns=columns)
    db.create_tables()
    db.clear_mappers()


def update_table(session, odds_tbl, odds_data):
    """Return a list of rows to update in the odds table.

    This function wraps updated rows from any number of functions that perform updates on different criteria."""
    line_updates = update_lines(session, odds_tbl, odds_data)

    return line_updates


def update_lines(session, odds_tbl, odds_data):
    """Update odds_tbl rows that are missing betting data present in the odds_data"""

    game_ids = odds_data.data['game_id']
    rows = session.query(odds_tbl).filter(odds_tbl.home_spread_price == None or odds_tbl.away_spread_price == None or
                                          odds_tbl.home_moneyline == None or odds_tbl.away_moneyline == None).filter(
                                          odds_tbl.game_id.in_(game_ids))

