"""ToDo: Add a global session maker"""

from datetime import datetime, timedelta
from datatotable.database import Database
from datatotable.data import DataOperator
from nbapredict.configuration import Config
from nbapredict.helpers.classes import NestedDict
from nbapredict.scrapers import team_scraper, line_scraper, season_scraper
import pandas
from sqlalchemy import ForeignKey, UniqueConstraint, func
from sqlalchemy.orm import aliased
import sqlalchemy.sql.selectable


def create_team_table(db, teams_data, tbl_name):
    """Create a table in DB named tbl_name with the columns in teams_data

    Args:
        db: a datotable.database.Database object connected to a database
        teams_data: A datatotable.data.DataOperator object with data on NBA teams
        tbl_name: The desired name of the table
    """
    columns = teams_data.columns
    columns["team_name"].append({"unique": True})
    db.map_table(tbl_name=tbl_name, columns=columns)
    db.create_tables()
    db.clear_mappers()


def create_team_stats_table(db, team_stats_data, tbl_name):
    """Create a table of team stats in a database with appropriate foreign keys and constraints.

    Args:
        db: a datotable.database.Database object connected to a database
        team_stats_data: A datatotable.data.DataOperator object with data on NBA team stats
        tbl_name: The desired table name
    ToDo: Currently allows duplicate rows if those values are on different days. Solve with a constraint
    """
    columns = team_stats_data.columns
    columns['team_id'].append(ForeignKey("teams_{}.id".format(Config.get_property('league_year'))))
    constraints = [UniqueConstraint("team_id", "scrape_time")]
    db.map_table(tbl_name=tbl_name, columns=columns, constraints=constraints)
    db.create_tables()
    db.clear_mappers()


def update_team_stats_table(db, session, team_stats_tbl, team_stats_data):
    """Insert new data into the team_stats_tbl.

    Args:
        db: a datotable.database.Database object connected to a database
        session: An instantiated SQLalchemy session object
        team_stats_tbl: A mapped team stats table object
        team_stats_data: A datatotable.data.DataOperator object with data on NBA team stats
    """
    last_insert_scrape_time = session.query(team_stats_tbl.scrape_time). \
        order_by(team_stats_tbl.scrape_time.desc()).first().scrape_time
    last_insert_date = datetime.date(last_insert_scrape_time)
    current_scrape_date = datetime.date(datetime.now())
    if last_insert_date < current_scrape_date:
        session.add_all([team_stats_tbl(**row) for row in team_stats_data.rows])
        session.commit()


def format_schedule_data(schedule_data, team_tbl, team_stats_tbl):
    """Format and return schedule data to match the database schema.

    Adds a Margin of Victory column and adds/modifies foreign key columns

    Args:
        schedule_data: A DataOperator object with schedule data
        team_tbl: A mapped instance of the team_tbl
        team_stats_tbl: A mapped instance of the team_stats_tbl
    """
    h_score = schedule_data.data['home_team_score']
    a_score = schedule_data.data['away_team_score']
    schedule_data.data['MOV'] = [h_score[i] - a_score[i] for i in range(schedule_data.num_rows())]
    schedule_data.data['playoffs'] = ['']
    schedule_data.fill('playoffs', None)

    schedule_data.data["home_team_id"] = values_to_foreign_key(team_tbl, "id", "team_name",
                                                               schedule_data.data.pop("home_team"))
    schedule_data.data["away_team_id"] = values_to_foreign_key(team_tbl, "id", "team_name",
                                                               schedule_data.data.pop("away_team"))

    today = datetime.date(datetime.now())
    tomorrow = today + timedelta(days=1)
    for idx in range(len(schedule_data.data['start_time'])):
        if schedule_data.data['start_time'][idx].date() >= tomorrow:
            tmrw_idx = idx
            break
    subquery = session.query(team_stats_tbl.id, team_stats_tbl.team_id, func.max(team_stats_tbl.scrape_time)). \
        filter(team_stats_tbl.scrape_date <= today).group_by(team_stats_tbl.team_id).subquery()
    schedule_data.data['home_stats_id'] = values_to_foreign_key(subquery, 'id', 'team_id',
                                                                schedule_data.data['home_team_id'][:tmrw_idx])
    schedule_data.data['away_stats_id'] = values_to_foreign_key(subquery, 'id', 'team_id',
                                                                schedule_data.data['away_team_id'][:tmrw_idx])
    schedule_data.fill('home_stats_id', None)
    schedule_data.fill('away_stats_id', None)

    return schedule_data


def create_schedule_table(db, schedule_data, tbl_name, team_tbl, team_stats_tbl):
    """Create a table of the NBA schedule in the database.
    Args:
        db: a datotable.database.Database object connected to a database
        schedule_data: A datatotable.data.DataOperator object with schedule data
        tbl_name: The desired name of the table
        team_tbl: A mapped team table to set foreign keys on
        team_stats_tbl: A mapped team stats table to set foreign keys on
    """
    columns = schedule_data.columns
    team_tbl_name = team_tbl.__table__.fullname
    team_stats_tbl_name = team_stats_tbl.__table__.fullname
    columns['home_team_id'].append(ForeignKey("{}.id".format(team_tbl_name)))
    columns['away_team_id'].append(ForeignKey("{}.id".format(team_tbl_name)))
    columns['home_stats_id'].append(ForeignKey("{}.id".format(team_stats_tbl_name)))
    columns['away_stats_id'].append(ForeignKey("{}.id".format(team_stats_tbl_name)))
    db.map_table(tbl_name=tbl_name, columns=columns)
    db.create_tables()
    db.clear_mappers()


def update_schedule_table(session, schedule_data, schedule_tbl, team_stats_tbl):
    score_updates = update_schedule_scores(session, schedule_data, schedule_tbl)
    stats_updates = update_schedule_stats(session, schedule_tbl, team_stats_tbl)

    # Some rows may be updated for scores and stats. Use a set to remove duplicates
    return set(score_updates + stats_updates)


def update_schedule_scores(session, schedule_data, schedule_tbl) -> list:
    date = datetime.date(datetime.now())
    update_query = session.query(schedule_tbl).filter(schedule_tbl.start_time < date,
                                                      schedule_tbl.home_team_score == 0). \
        order_by(schedule_tbl.start_time)
    # if update_query.count() == 0:
    #     return
    rows = update_query.all()
    if len(rows) == 0:
        return []
    first_game_time = rows[0].start_time
    last_game_time = rows[len(rows) - 1].start_time

    sched_df = schedule_data.dataframe
    sched_df["start_time"] = sched_df["start_time"].dt.tz_localize(None)
    update_df = sched_df.loc[(sched_df.start_time >= first_game_time) & (sched_df.start_time <= last_game_time)]

    update_rows = []
    for row in rows:
        game = update_df.loc[(update_df.home_team_id == row.home_team_id) & (update_df.away_team_id == row.away_team_id)
                             & (update_df.start_time.dt.date == datetime.date(row.start_time))]
        row.home_team_score = int(game.home_team_score)
        row.away_team_score = int(game.away_team_score)
        row.MOV = row.home_team_score - row.away_team_score
        row.start_time = game.start_time.dt.to_pydatetime()[0]  # Convert Pandas TimeStamp to datetime
        update_rows.append(row)
    return update_rows


def update_schedule_stats(session, schedule_tbl, team_stats_tbl) -> list:
    tomorrow = datetime.date(datetime.now()) + timedelta(days=1)

    d_time = session.query(func.min(schedule_tbl.start_time)).filter(schedule_tbl.home_stats_id == None).all()[0][0]
    date = datetime.date(d_time)
    date_ranges = []
    while date < tomorrow:
        next_day = date + timedelta(days=1)
        date_ranges.append((date, next_day))
        date = next_day

    update_rows = []
    for d in date_ranges:
        # Get the team stats with the greatest scrape_time before the end date of the range (31 obs, all teams + L. AVG)
        stats_q = session.query(team_stats_tbl.id, team_stats_tbl.team_id,
                                func.max(team_stats_tbl.scrape_time).label('s_time')). \
            filter(team_stats_tbl.scrape_time < d[1]).group_by(team_stats_tbl.team_id).subquery()
        home_stats = aliased(stats_q, 'home_stats')
        away_stats = aliased(stats_q, 'away_stats')

        sched_rows = session.query(schedule_tbl, home_stats.c.id.label('h_s_id'), away_stats.c.id.label('a_s_id')). \
            filter(schedule_tbl.home_stats_id == None, schedule_tbl.start_time > d[0], schedule_tbl.start_time < d[1]). \
            join(home_stats, schedule_tbl.home_team_id == home_stats.c.team_id). \
            join(away_stats, schedule_tbl.away_team_id == away_stats.c.team_id).all()

        for row in sched_rows:
            row.schedule_2020.home_stats_id = row.a_s_id
            row.schedule_2020.away_stats_id = row.h_s_id
            update_rows.append(row.schedule_2020)
    return update_rows


def format_odds_data(odds_dict, team_tbl, schedule_tbl):
    """From the odds_dict, strip extraneous dictionary keys, add a 'game_id' FK, and return the odds_dict

    Args:
        odds_dict: A dictionary of data returned by line_scraper
        team_tbl: A mapped team table
        schedule_tbl: A mapped schedule table

    Returns:
        odds_dict formatted with foreign keys (mainly a FK for games in the schedule tbl)
    """
    odds_dict['home_team_id'] = values_to_foreign_key(team_tbl, "id", 'team_name', odds_dict.pop('home_team'))
    # the columns that uniquely identify a game in the schedule table
    val_cols = ['home_team_id', 'start_time']
    uID = {k: odds_dict[k] for k in val_cols}  # Home team + start_time form a unique identifier for a game in schedule
    odds_dict['game_id'] = values_to_foreign_key(schedule_tbl, "id", val_cols, uID)

    # Each of these columns is held in the schedule table in the row game_id references
    del odds_dict['start_time']
    del odds_dict['away_team']
    del odds_dict['home_team_id']

    return odds_dict


def create_odds_table(db, tbl_name, odds_data, schedule_tbl):
    """Create a table of odds in the database"""
    columns = odds_data.columns
    schedule_tbl_name = schedule_tbl.__table__.fullname
    columns['game_id'].append(ForeignKey("{}.id".format(schedule_tbl_name)))
    db.map_table(tbl_name=tbl_name, columns=columns)
    db.create_tables()
    db.clear_mappers()


def values_to_foreign_key(foreign_tbl, foreign_key, foreign_value, child_data):
    """Return values from child data that exist in the foreign_tbl transformed into foreign key values

    Args:
        foreign_tbl: The foreign table mapping child data references
        foreign_key: The name of the column containing foreign key values
        foreign_value: The name of the column containing values to match with child data
        child_data: A list of data with values contained in foreign value

    Returns:
         A list of values from the foreign key column that correspond to child data's relationship to the foreign values
    """
    # past 999 the SQLite backend raises a "too many variables warning". Here, we presume we don't have >999 unique
    # values in child_data. Rather, presume we have < 999 unique values and take a set of the data.
    set_data = set()
    if len(child_data) > 999:
        set_data = set(child_data)
    if type(foreign_tbl) == sqlalchemy.sql.selectable.Alias:
        conversion_dict = _values_to_foreign_key(foreign_tbl, foreign_key, foreign_value, set_data or child_data)
        return [conversion_dict[i] for i in child_data]
    else:
        key_column = [getattr(foreign_tbl, foreign_key)]
        if isinstance(child_data, dict):
            composite_fd = True  # Composite foreign dependency, two columns required to identify unique key
            value_columns = [getattr(foreign_tbl, val) for val in child_data.keys()]
            keys = list(child_data.keys())
            filters = [value_columns[i].in_(child_data[keys[i]]) for i in range(len(keys))]
        else:
            composite_fd = False
            value_columns = [getattr(foreign_tbl, foreign_value)]
            filters = [value_columns[0].in_(set_data or child_data)]

        rows = session.query(*key_column, *value_columns).distinct().filter(*filters).all()
        # rows = session.query(getattr(foreign_tbl, foreign_key), getattr(foreign_tbl, foreign_value)). \
        # filter(getattr(foreign_tbl, foreign_value).in_(set_data or child_data)).all()
        if composite_fd:
            l = len(rows[0])  # num. columns
            d = NestedDict()
            for r in rows:
                d[[col for col in r[1:]]] = r[0]  # multi-valued key with the foreign key as the value

            # Convert to df to facilitate row selection
            df = pandas.DataFrame(child_data)
            multi_keys = []
            for i in range(len(df)):
                row = df.iloc[i]
                multi_keys.append([i for i in row])
            return [d[keys] for keys in multi_keys]
        else:
            conversion_dict = {getattr(row, foreign_value): getattr(row, foreign_key) for row in rows}
            return [conversion_dict[i] for i in child_data]


def _values_to_foreign_key(foreign_subquery, foreign_key, foreign_value, child_data):
    """Return values from child data that exist in the foreign_subquery transformed into foreign key values

    This function performs the same query as values_to_foreign_key() except it can take a subquery, which has
    different syntax, as input rather than a table. The function presumes child_data has already been modified if
    necessary. NOTE: this does not support multi-column conversions of child_data to foreign key.

    Args:
        foreign_subquery: A subquery which is an Alias class in sqlalchemy. These classes are created when subquery()
        is appended to a sqlalchemy query statement
        foreign_key: The name of the column containing foreign key values
        foreign_value: The name of the column containing values to match with child data
        child_data: A list of data with values contained in foreign value

    Returns:
         A conversion dict that maps child_data to foreign keys
    """
    rows = session.query(getattr(foreign_subquery.c, foreign_key), getattr(foreign_subquery.c, foreign_value)). \
        filter(getattr(foreign_subquery.c, foreign_value).in_(child_data)).all()
    conversion_dict = {getattr(row, foreign_value): getattr(row, foreign_key) for row in rows}
    return conversion_dict


def main(db, session):
    year = Config.get_property("league_year")

    # ~~~~~~~~~~~~~
    # Teams
    # ~~~~~~~~~~~~~
    team_dict = team_scraper.scrape()
    teams_data = DataOperator({"team_name": team_dict["team_name"]})
    teams_tbl_name = "teams_{}".format(year)
    if not db.table_exists(teams_tbl_name):
        create_team_table(db=db, teams_data=teams_data, tbl_name=teams_tbl_name)
        teams_tbl = db.table_mappings[teams_tbl_name]
        session.add_all([teams_tbl(**row) for row in teams_data.rows])
        session.commit()
        del teams_tbl

    # ~~~~~~~~~~~~~
    # Team Stats
    # ~~~~~~~~~~~~~
    team_stats_tbl_name = "team_stats_{}".format(year)
    teams_tbl = db.table_mappings[teams_tbl_name]
    team_dict['team_id'] = team_dict.pop('team_name')
    team_dict['team_id'] = values_to_foreign_key(foreign_tbl=teams_tbl, foreign_key="id", foreign_value="team_name",
                                                 child_data=team_dict['team_id'])
    # When team_stats_tbl is created, the teams_tbl automap object is changed. The changed format does not follow
    # the expected behavior of an automapped table. I suspect this is because a relationship is established.
    # If we reloaded, teams_tbl works fine. Therefore, delete the variable here for now
    del teams_tbl
    team_dict['scrape_date'] = [datetime.date(s_time) for s_time in team_dict['scrape_time']]
    team_stats_data = DataOperator(team_dict)
    if not db.table_exists(team_stats_tbl_name):
        create_team_stats_table(db=db, team_stats_data=team_stats_data, tbl_name=team_stats_tbl_name)
        team_stats_tbl = db.table_mappings[team_stats_tbl_name]
        session.add_all([team_stats_tbl(**row) for row in team_stats_data.rows])
        session.commit()
    else:
        team_stats_tbl = db.table_mappings[team_stats_tbl_name]
        update_team_stats_table(db, session, team_stats_tbl, team_stats_data)

    # ~~~~~~~~~~~~~
    # Schedule
    # ~~~~~~~~~~~~~
    schedule_dict = season_scraper.scrape()
    schedule_data = DataOperator(schedule_dict)
    teams_tbl = db.table_mappings['teams_{}'.format(year)]
    schedule_data = format_schedule_data(schedule_data, teams_tbl, team_stats_tbl)
    schedule_tbl_name = "schedule_{}".format(year)
    if not db.table_exists(schedule_tbl_name):
        create_schedule_table(db, schedule_data, schedule_tbl_name, teams_tbl, team_stats_tbl)
        schedule_tbl = db.table_mappings[schedule_tbl_name]
        session.add_all([schedule_tbl(**row) for row in schedule_data.rows])
        session.commit()
    else:
        schedule_tbl = db.table_mappings[schedule_tbl_name]
        update_rows = update_schedule_table(session, schedule_data, schedule_tbl, team_stats_tbl)
        session.add_all(update_rows)
        session.commit()

    # ~~~~~~~~~~~~~
    # Odds
    # ~~~~~~~~~~~~~
    odds_dict = line_scraper.scrape()
    if odds_dict:
        odds_dict = format_odds_data(odds_dict, teams_tbl, schedule_tbl)
        odds_data = DataOperator(odds_dict)
    # Evaluate if you have the correct columns in odds_data (i.e. home\away team id's)
    odds_tbl_name = "odds_{}".format(year)
    if not db.table_exists(odds_tbl_name):
        create_odds_table(db, odds_tbl_name, odds_data, schedule_tbl)
    t = 2


if __name__ == "__main__":
    from sqlalchemy.orm import Session

    db = Database("test", Config.get_property("outputs"))
    session = Session(db.engine)
    main(db, session)
    t = 2
