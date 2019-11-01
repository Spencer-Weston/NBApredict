from datetime import datetime
from datatotable.database import Database
from datatotable.data import DataOperator
from nbapredict.scrapers import team_scraper, line_scraper, season_scraper
from nbapredict.configuration import Config
from sqlalchemy import ForeignKey, UniqueConstraint, func
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
        team_tbl: A mapped table object of the teams table
        tbl_name: The desired table name
    ToDo: Change Unique Constraint to use new format once datatotable update is incorporated
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
    schedule_data.data['MOV'] = [h_score[i]-a_score[i] for i in range(schedule_data.num_rows())]
    schedule_data.data['playoffs'] = ['']
    schedule_data.fill('playoffs', None)

    schedule_data.data["home_team_id"] = values_to_foreign_key(team_tbl, "id", "team_name",
                                                               schedule_data.data.pop("home_team"))
    schedule_data.data["away_team_id"] = values_to_foreign_key(team_tbl, "id", "team_name",
                                                               schedule_data.data.pop("away_team"))

    today = datetime.date(datetime.now())
    subquery = session.query(team_stats_tbl.id, team_stats_tbl.team_id, func.max(team_stats_tbl.scrape_time)). \
        filter(team_stats_tbl.scrape_date <= today).group_by(team_stats_tbl.team_id).subquery()
    schedule_data.data['home_stats_id'] = values_to_foreign_key(subquery, 'id', 'team_id',
                                                                schedule_data.data['home_team_id'])
    schedule_data.data['away_stats_id'] = values_to_foreign_key(subquery, 'id', 'team_id',
                                                                schedule_data.data['away_team_id'])

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
    # observations in child_data. Rather, presume we have < 999 unique observations and take a set of the data.
    set_data = set()
    if len(child_data) > 999:
        set_data = set(child_data)
    if type(foreign_tbl) == sqlalchemy.sql.selectable.Alias:
        conversion_dict = _values_to_foreign_key(foreign_tbl, foreign_key, foreign_value, set_data or child_data)
    else:
        rows = session.query(getattr(foreign_tbl, foreign_key), getattr(foreign_tbl, foreign_value)).\
            filter(getattr(foreign_tbl, foreign_value).in_(set_data or child_data)).all()
        conversion_dict = {getattr(row, foreign_value): getattr(row, foreign_key) for row in rows}
    return [conversion_dict[i] for i in child_data]


def _values_to_foreign_key(foreign_subquery, foreign_key, foreign_value, child_data):
    """Return values from child data that exist in the foreign_subquery transformed into foreign key values

    This function performs the same query as values_to_foreign_key() except it can take a subquery, which has
    different syntax, as input rather than a table. The function presumes child_data has already been modified if
    necessary.

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
    team_dict = team_scraper.scrape()
    teams_data = DataOperator({"team_name": team_dict["team_name"]})

    # Teams
    teams_tbl_name = "teams_{}".format(year)
    if not db.table_exists(teams_tbl_name):
        create_team_table(db=db, teams_data=teams_data, tbl_name=teams_tbl_name)
        teams_tbl = db.table_mappings[teams_tbl_name]
        session.add_all([teams_tbl(**row) for row in teams_data.rows])
        session.commit()
        del teams_tbl

    # Team Stats
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
        # The following inserts new rows into the database if the current data was scraped on a later date than the
        # last insert into the database.
        team_stats_tbl = db.table_mappings[team_stats_tbl_name]
        update_team_stats_table(db, session, team_stats_tbl, team_stats_data)

    # Schedule
    schedule_dict = season_scraper.scrape()
    schedule_data = DataOperator(schedule_dict)
    teams_tbl = db.table_mappings['teams_{}'.format(year)]
    schedule_data = format_schedule_data(schedule_data, teams_tbl, team_stats_tbl)
    schedule_tbl_name = "schedule_{}".format(year)
    if not db.table_exists(schedule_tbl_name):
        create_schedule_table(db, schedule_data, schedule_tbl_name, teams_tbl, team_stats_tbl)
        # schedule_tbl = db.table_mappings[schedule_tbl_name]
    else:
        schedule_tbl = db.table_mappings[schedule_tbl_name]
        session.add_all([schedule_tbl(**row) for row in schedule_data.rows])
        session.commit()
        # update_schedule_table()
    schedule_tbl = db.table_mappings[schedule_tbl_name]
    session.add_all([schedule_tbl(**row) for row in schedule_data.rows])
    session.commit()
    t=2


if __name__ == "__main__":
    from sqlalchemy.orm import Session

    db = Database("test", Config.get_property("outputs"))
    session = Session(db.engine)
    main(db, session)
    t = 2
