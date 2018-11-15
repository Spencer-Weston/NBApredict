from sqlalchemy import create_engine
from sqlalchemy import Table
from sqlalchemy import MetaData
from sqlalchemy import inspect
from sqlalchemy import Column, Integer, Float, String
from sqlalchemy.ext.declarative import declarative_base


# Test Vars
test_case = {"team": "Bucks", "wins": 2, "losses": 1}
tbl_name = "bucks_test"
type_case = {"team": String, "wins": Integer, "losses": Integer}
# Database Setup
db_url = "sqlite:///database//nba_db.db"
engine = create_engine(db_url)
Base = declarative_base()




cols = create_col_definitions(tbl_name, type_case)
create_table(tbl_name, cols=cols)

inspector = inspect(engine)

for _t in inspector.get_table_names():
    print(_t)

inspector.get_columns("bucks_test")

metadata = MetaData(engine, reflect= True)
print(metadata.tables)

bucks_tbl = metadata.tables["bucks_test"]
print(bucks_tbl)

print("chimichanga")