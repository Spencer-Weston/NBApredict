"""Team_stats.py contains function to create the team_stats table in the database"""

from datetime import datetime
from nbapredict.configuration import Config
from sqlalchemy import ForeignKey, UniqueConstraint


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


def insert_team_stats_table(session, team_stats_tbl, team_stats_data):
    """Insert new data into the team_stats_tbl.

    Args:
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
