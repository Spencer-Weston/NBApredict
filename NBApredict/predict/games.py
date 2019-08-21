"""Predict.games contains functions oriented around predicting games"""

from sqlalchemy import Integer, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Session, relationship


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


if __name__ == "__main__":
    pass