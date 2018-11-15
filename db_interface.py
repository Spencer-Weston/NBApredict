import sqlite3
import general
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, String


# Type conversion functions
def get_py_type(tbl_dict):
    """Given a table dictionary (formatted as key:[list]), returns a dictionary of tbl_key: py_type where py_type can
    be 'integer', 'float', or 'string' (at the moment)"""

    tbl_keys = list(tbl_dict.keys())
    py_types = [general.get_type(tbl_dict[key]) for key in list(tbl_dict.keys())]
    py_types_dict = dict(zip(tbl_keys, py_types))
    return py_types_dict


def get_sql_type(tbl_dict):
    """Given a table dictionary (formatted as key:[list]), returns a dictionary of tbl_key: sql_type
    where sql_type is a sqlalchemy sql_type class"""

    py_types = get_py_type(tbl_dict) #py_types is a dict
    sql_types = py_type_to_sql_type(py_types)
    return(sql_types)


def py_type_to_sql_type(py_types):
    """Converts a dictionary of python types to a dictionary of sql types where sql type is a sqlalchemy sql_type
    class"""

    sql_types = dict()
    for key in py_types:
        if py_types[key] == "integer":
            sql_types[key] = Integer
        elif py_types[key] == "float":
            sql_types[key] = Float
        elif py_types[key] == "string":
            sql_types[key] = String
        else:
            raise Exception("Error: py_type {} is not an integer, float, or string".format(py_types[key]))
    return sql_types


# Database interaction functions

Base = declarative_base()

def create_table(engine, name, cols):
    Base.metadata.reflect(engine)
    if name in Base.metadata.tables: return
    table = type(name, (Base,), cols)
    table.__table__.create(bind=engine)


def create_col_definitions(tbl_name, id_type_dict):
    col_specs = {'__tablename__': '{}'.format(tbl_name),
                 'id': Column(Integer, primary_key=True)}
    for key in id_type_dict:
        col_specs[key] = Column(id_type_dict[key])

    return col_specs