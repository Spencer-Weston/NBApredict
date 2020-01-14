"""Teams.py contains (a) function(s) to create the teams table in the database"""


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
