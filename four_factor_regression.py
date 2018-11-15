import database as db
from sqlalchemy import create_engine
from sqlalchemy import select
import classification_dicts as cd
import pandas as pd

# Variable setup
db_url = "sqlite:///database//nba_db.db"
engine = create_engine(db_url)
conn = engine.connect()

tbl_name = "misc_stats"
four_factors = cd.four_factors.insert(0, "team_name")

# Database table to pandas table
ff_df = pd.read_sql_table(tbl_name, conn)[four_factors]  # FF = four factors
