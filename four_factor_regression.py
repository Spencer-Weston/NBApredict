import database as db
from sqlalchemy import create_engine
from sqlalchemy import select
import classification_dicts as cd
import pandas as pd
from sklearn import linear_model

# Variable setup
db_url = "sqlite:///database//nba_db.db"
engine = create_engine(db_url)
conn = engine.connect()

# Import and specify a list of factors to extract from database
ff_list = cd.four_factors

target_list = []
ff_list.insert(0, "team_name")
ff_list.append("wins")
ff_list.append("losses")

# Database table to pandas table
tbl_name = "misc_stats"
ff_df = pd.read_sql_table(tbl_name, conn)[ff_list]  # FF = four factors

print("FINISHED")