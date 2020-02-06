"""odds.py contains function to create the odds table in the database"""

import nbapredict.management.conversion as convert
from sqlalchemy import ForeignKey, or_
from datetime import timedelta
import math


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
    odds_dict = check_gametimes(session, schedule_tbl, odds_dict)

    # the columns that uniquely identify a game in the schedule table
    val_cols = ['home_team_id', 'start_time']
    uID = {k: odds_dict[k] for k in val_cols}  # Home team + start_time form a unique identifier for a game in schedule
    odds_dict['game_id'] = convert.values_to_foreign_key(session, schedule_tbl, "id", val_cols, uID)

    # Each of these columns is held in the schedule table
    del odds_dict['start_time']
    del odds_dict['away_team']
    del odds_dict['home_team_id']

    return odds_dict


def check_gametimes(session, schedule_tbl, odds_dict):
    """Check and, if necessary, change game times in the odds_dict

    Some games in Bovada do not have the same time as those in the official schedule. For example a Bovada game may
    start at 9:05 whereas the official game time is 9:00. """
    first_gametime = min(odds_dict['start_time']) - timedelta(hours=12)
    last_gametime = max(odds_dict['start_time']) + timedelta(days=1)
    sched_times = session.query(schedule_tbl.start_time).filter(
        schedule_tbl.home_team_id.in_(odds_dict['home_team_id']),
        schedule_tbl.start_time >= first_gametime,
        schedule_tbl.start_time <= last_gametime).all()
    sched_times = [t.start_time for t in sched_times]

    s_times = odds_dict['start_time']
    # List of tuples where the first element is the index to replace in odds_dict and the second element is the
    # unmatched time
    unmatched_times = [(t, s_times[t]) for t in range(len(s_times)) if s_times[t] not in sched_times]
    offsets = [timedelta(minutes=5)]  # Append more offsets here if they arise in the future

    # Check if the unmatched times +/- an offset exists in the schedule times
    for i in unmatched_times:
        for j in offsets:
            if (i[1] + j) in sched_times:
                odds_dict['start_time'][i[0]] = i[1] + j
                break
            elif (i[1] - j) in sched_times:
                odds_dict['start_time'][i[0]] = i[1] - j
                break

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
    rows = session.query(odds_tbl).filter(or_(odds_tbl.home_spread_price == None, odds_tbl.away_spread_price == None,
                                              odds_tbl.home_moneyline == None, odds_tbl.away_moneyline == None) &
                                          odds_tbl.game_id.in_(game_ids))
    if rows.count() > 0:
        rows = rows.all()
        data_df = odds_data.dataframe
        update_rows = []
        bet_cols = ['home_spread_price', 'away_spread_price', 'home_moneyline', 'away_moneyline', 'spread']
        for r in rows:
            data_row = data_df[data_df['game_id'] == r.game_id]
            updated = False
            for c in bet_cols:
                data_val = data_row[c].to_numpy()[0]
                if math.isnan(data_val):
                    data_val = None
                if data_val != getattr(r, c):
                    setattr(r, c, data_val)
                    updated = True
            if updated:
                update_rows.append(r)
    else:
        update_rows = []
    return(update_rows)
