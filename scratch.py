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


def create_table(name, cols):
    Base.metadata.reflect(engine)
    if name in Base.metadata.tables: return
    table = type(name, (Base,), cols)
    table.__table__.create(bind=engine)


def create_col_definitions(tbl_name, id_type_dict):
    """Returns a dictionary that begins with __table__name and an integer id followed by columns as specified
    in the id_type_dict
    tbl_name: name of the desired table
    id_type_dict: dictionary of column id's and associated sql_alchemy sql types"""

    col_specs = {'__tablename__': '{}'.format(tbl_name),
                 'id': Column(Integer, primary_key=True)}
    for key in id_type_dict:
        col_specs[key] = Column(id_type_dict[key])

    return col_specs

cols = create_col_definitions(tbl_name, type_case)
create_table(tbl_name, cols=cols)

inspector = inspect(engine)

for _t in inspector.get_table_names():
    print(_t)

inspector.get_columns("bucks_test")

metadata = MetaData(engine)
print(metadata.tables)

bucks_tbl = metadata.tables["bucks_test"]
print(bucks_tbl)

print("chimichanga")