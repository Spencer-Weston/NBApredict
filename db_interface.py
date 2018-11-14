import sqlite3
import general
from sqlalchemy import create_engine

def db_connect(db_path):
    """Returns a connection to the specified db"""
    conn = sqlite3.connect(db_path)
    return conn

def create_table_from_dict(conn, tbl_name, tbl_dict, overwrite = True):
    """Creates a sql table from a dictionary
    conn - sql db connection
    tbl_name - name of the requested table
    tbl_dict - dictionary to insert into table"""

    colnames = list(tbl_dict.keys())
    py_types = []
    for name in colnames:
        py_types.append(general.get_type(tbl_dict[name]))
    coltypes = py_type_to_sql_type(py_types)

    query = create_tbl_query(tbl_name, colnames, coltypes, overwrite)
    conn.execute(query)


def create_tbl_query(tbl_name, colnames, coltypes, overwrite):

    cols_string = ""
    for idx, val in enumerate(colnames):
        if idx != len(colnames):  # if not the last index
            cols_string = cols_string + colnames[idx] + " " + coltypes[idx] + ", "
        else:  # if the last index
            cols_string = cols_string + colnames[idx] + " " + coltypes[idx]
    if overwrite:
        query = """ CREATE TABLE "{}" (
                                            {}
                                        );""".format(tbl_name, cols_string)
    else:
        query = """ CREATE TABLE IF NOT EXISTS "{}" (
                                                    {}
                                                );""".format(tbl_name, cols_string)
    return query


def py_type_to_sql_type(py_types):
    """Converts a list of python types to a list of sql types"""

    sql_types = []
    for val in py_types:
        if val == "integer":
            sql_types.append("INTEGER")
        elif val == "float":
            sql_types.append("REAL")
        elif val == "string":
            sql_types.append("TEXT")
        else:
            raise Exception("Error: no modal type found in list")
    return sql_types


if __name__ == "__main__":
    db = "database/nba_db.db"
    test = db_connect(db)
    print("bleghghg")