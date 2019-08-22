"""Predict.games contains functions oriented around predicting games"""

from sqlalchemy import Integer, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Session, relationship

# Local Imports
from . import get
from nbapredict.configuration import Config
import nbapredict.models.four_factor_regression as lm
import nbapredict.database.dbinterface as dbinterface


def get_sample_prediction(database, session, sched_tbl, regression):
    """Generate and return a sample prediction to create a prediction table from.

    Args:
        database: An initialized DBInterface class from database.dbinterface.py
        session: A SQLalchemy session object
        sched_tbl: A mapped schedule table
        regression: A regression object from four_factor_regression.py

    Returns:
        A DataManipulator object initialized with a prediction from regression
    """
    first_game_odds = session.query(sched_tbl).order_by(sched_tbl.start_time).first()

    home_tm = first_game_odds.home_team
    away_tm = first_game_odds.away_team
    start_time = first_game_odds.start_time
    line = first_game_odds.spread

    sample_prediction = predict_game(database, session, regression, home_tm, away_tm, start_time, line)
    data = DataManipulator(sample_prediction)
    return data

def create_prediction_table(database, data, tbl_name):
    """Create a prediction table from the data and with the table name in the database.

    Args:
        database: An initialized DBInterface class from database.dbinterface.py
        data: An initialized DataManipulator object, from database.manipulator, with prediction data
        tbl_name: The desired table name (with year as the last four characters)
    """
    # Create columns from data
    sql_types = data.get_sql_type()
    # Add new columns
    year = tbl_name[-4:]
    schedule_name = "sched_{}".format(year)
    additional_cols = [{'game_id': [Integer, ForeignKey(schedule_name + ".id")]},
                       {"home_team_score": Integer},
                       {"away_team_score": Integer}]
    for col in additional_cols:
        sql_types.update(col)
    constraint = {UniqueConstraint: ["start_time", "home_team", "away_team"]}
    # Map prediction table
    database.map_table(tbl_name, sql_types, constraint)

    # Get tables for relationships
    sched_tbl = database.get_table_mappings(schedule_name)

    # Create Relationships
    if "predictions" not in sched_tbl.__mapper__.relationships.keys():
        sched_tbl.predictions = relationship(database.Template)

    database.create_tables()

def main():
    db = dbinterface.DBInterface()
    session = Session(bind=db.engine)

    regression = lm.main(db, session)
    sched_tbl = db.get_table_mappings("sched_{}".format(Config.get_property("league_year")))

    if not db.table_exists("pred")



if __name__ == "__main__":
    pass