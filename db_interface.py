import sqlite3
import general

def db_connect(db_path):
    """Returns a connection to the specified db"""
    conn = sqlite3.connect(db_path)
    return conn

def create_table_from_dict(conn, tbl_name, tbl_dict, overwrite = True):
    """Creates a sql table from a dictionary
    conn - sql db connection
    tbl_name - name of the requested table
    tbl_dict - dictionary to insert into table"""

    colnames = tbl_dict.keys()
    coltypes = []
    for name in colnames:
        coltypes.append(general.get_type(tbl_dict[name]))
    coltypes = py_type_to_sql_type(coltypes)

    query = create_tbl_query(tbl_name, colnames, coltypes, overwrite)


def create_tbl_query(tbl_name, colnames, coltypes, overwrite):

    cols_string = ""
    if overwrite:
        query = """ CREATE TABLE {} (
                                            {}
                                        ); """.format(tbl_name, cols_string)
    else:
        query = """ CREATE TABLE IF NOT EXISTS {} (
                                                    {}
                                                ); """.format(tbl_name, cols_string)
    return query


def py_type_to_sql_type(py_types):
    """Converts a list of python types to a list of sql types"""

    sql_types = []
    for val in values:
        if val is int:
            sql_types.append("integer")
        if val is float:
            sql_types.append("real")
        if val is str:
            sql_types.append("text")
    return sql_types


if __name__ == "__main__":
    db = "database/nba_db.db"
    test = db_connect(db)
    print("bleghghg")