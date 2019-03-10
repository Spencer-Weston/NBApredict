"""
Author: Spencer Weston

Purpose: This file contains functions for working with databases. It works with SQL lite databases and uses the
sqlalchemy package to construct the functions.

To-do:
    In all likelihood, many of these functions should be contained in a class. The conversion will need to be studied
    a bit to figure out the optimal structure. But in general, a database class should have access to all tables within
    it. Those tables should likely be classes themselves with associated functions.
"""

import datetime
from sqlalchemy import Column, ForeignKey, Integer, Float, String, DateTime, Boolean, Table, UniqueConstraint
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import mapper, clear_mappers
from sqlalchemy.ext.automap import automap_base

# local imports
import general


def create_col_definitions(tbl_name, id_type_dict, foreign_key=False):
    """Create and return a dictionary of column specifications for a sql_alchemy table.

    Returns a dictionary that begins with __table__name and an integer id followed by columns as specified
    in the id_type_dict

    Args:
        tbl_name: name of the table to create column definitions for
        id_type_dict: dictionary of column id's as keys and sql_alchemy sql types as values
        foreign_key: To add a foreign key column, pass a string formatted as foreign "table.column"
    Returns:
        Column definitions
    """
    col_specs = {'__tablename__': '{}'.format(tbl_name),
                 'id': Column(Integer, primary_key=True)}
    if foreign_key:
        col_specs[foreign_key] = Column(foreign_key, Integer, ForeignKey(foreign_key), nullable=False, unique=True)
    for key in id_type_dict:
        col_specs[key] = Column(id_type_dict[key])

    return col_specs


class Database:
    """Database is the highest level in a hierarchy of classes which deal with database interactions with sqlalchemy

    Database serves as the hub for high level attributes such as the Base and engine which apply to all tables in the
    database. """

    class Template(object):
        """Blank template to map tables to with the sqlalchemy mapper function"""
        pass

    def __init__(self, url):
        """Creates an engine on the specified path, generates a declarative Base class which reflects the engine
        The base is passed to subclasses in order to generate tables. The session allows interaction with the DB."""
        self.path = url
        self.engine = create_engine(self.path)
        # self.conn = self.engine.connect()
        self.metadata = MetaData(self.engine)
        self.Base = declarative_base()
        self.table_names = list(self.get_tables())

    def get_tables(self, table_names=False):
        """Find and return the specified tables or return all tables """
        meta = MetaData(bind=self.engine)
        meta.reflect(bind=self.engine)
        if table_names:
            return meta.tables[table_names]
        else:
            return meta.tables

    def get_table_mappings(self, table_names: list):
        """Find and return the specified table mappings or return all table mappings

        Args:
         table_names: The list of table names for which mappings are desired"""
        self.metadata.reflect(self.engine, only=table_names)
        Base = automap_base(metadata=self.metadata)
        Base.prepare()
        mapped_tables = [Base.classes[name] for name in table_names]
        if len(mapped_tables) == 1:
            return mapped_tables[0]
        else:
            return mapped_tables

    def table_exists(self, tbl_name):
        self.metadata.reflect(bind=self.engine)
        if tbl_name in self.metadata.tables:
            return True
        else:
            return False

    def create_tables(self):
        """Creates all tables which have been made with the Base class of the Database"""
        self.metadata.create_all(self.engine)

    def map_table(self, tbl_name, column_types, constraints=False):
        """Maps a dictionary keyed on column names with Type values and, optionally, constraints

        Args:
            tbl_name: The name of the table to be mapped
            column_types: A dictionary with column names as keys and sql types as values
            constraints: A dictionary of desired constraints where the constraints (Such as UniqueConstraint) are keys
            and the columns to be constrained is a list of string column names
        """
        if constraints:

            t = Table(tbl_name, self.metadata, Column('id', Integer, primary_key=True),
                      *(Column(key, value) for key, value in column_types.items()),
                      *(constraint(*columns) for constraint, columns in constraints.items())
                      )

        else:
            t = Table(tbl_name, self.metadata, Column('id', Integer, primary_key=True),
                      *(Column(key, value) for key, value in column_types.items()))

        mapper(self.Template, t)

    @staticmethod
    def clear_mappers():
        clear_mappers()

    def insert_row(self, table, row):
        """Insert a single row into the specified table in the engine"""
        conn = self.engine.connect()
        table = self.get_tables(table)
        conn.execute(table.insert(), row)
        conn.close()
        # Rows formatted as
        #   [{'l_name': 'Jones', 'f_name': 'bob'},
        #   {'l_name': 'Welker', 'f_name': 'alice'}])

    def insert_rows(self, table, rows):
        """Insert rows into the specified table.

        Uses sqlalchemy's "Classic" method. Could also be done with a session.
        """
        table = self.get_tables(table)
        conn = self.engine.connect()
        for row in rows:
            conn.execute(table.insert(), row)
        conn.close()

    def drop_table(self, drop_tbl):
        """Drops the specified table from the database"""

        self.metadata.reflect(bind=self.engine)
        drop_tbls = self.metadata.tables[drop_tbl]
        drop_tbls.drop()
        self.metadata = MetaData(bind=self.engine)  # Updates the metadata to reflect changes


class DataManipulator:
    """DataManipulator takes scraped data in init, and uses its member functions to return manipulations of that data"""

    def __init__(self, data_dict):
        """Stores the data dictionary passed to it

        Args:
            data_dict: A dictionary of data which will, usually, reflect data scraped from a website"""
        self.data = data_dict
        self.rows = None

    def get_sql_type(self):
        """Take the object's dictionary {key: [values]} and return a dictionary formatted as {key: SQLtype}


        Returns:
            A dictionary with the same keys as tbl_dict. The dictionary's values are the sql_types of each key:value
            pair in tbl_dict. The sql_types are defined to function with sqlalchemy.
        """
        py_types = self._get_py_type()  # py_types is a dict
        sql_types = self._py_type_to_sql_type(py_types)
        return sql_types

    # Type conversion functions (python types, sql types, sqlalchemy types)
    def _get_py_type(self):
        """Take the classes data values and return a dictionary that holds the python type for the values.

        Returns:
            A dictionary formatted as key:py_type where the type can be integer, float, string, datetime, or none
        """
        py_types_dict = {}
        if isinstance(self.data, dict):
            tbl_keys = list(self.data.keys())
            py_types = [general.get_type(self.data[key]) for key in tbl_keys]
            py_types_dict = dict(zip(tbl_keys, py_types))
        elif isinstance(self.data, list):
            if isinstance(self.data[0], dict):
                data = self.data[0]
                tbl_keys = list(data.keys())
                py_types = [general.get_type(data[key]) for key in tbl_keys]
                py_types_dict = dict(zip(tbl_keys, py_types))
            else:
                raise Exception("The data structure ({}) is by _get_py_type".format(type(self.data)))
        return py_types_dict

    @staticmethod
    def _py_type_to_sql_type(py_types):
        """Convert and return a dictionary of python types to a dictionary of sql types.

        Raises:
            An exception if a py_type is not an integer, float, string, datetime, bool, or none

        To-do:
            Change the logic into a switch statement
        """

        sql_types = dict()
        for key in py_types:
            py_type = py_types[key]
            if py_type == "integer" or py_type is int:
                sql_types[key] = Integer
            elif py_type == "float" or py_type is float:
                sql_types[key] = Float
            elif py_type == "string" or py_type is str:
                sql_types[key] = String
            elif py_type == "datetime" or py_type is datetime.datetime:
                sql_types[key] = DateTime
            elif py_type == "bool" or py_type is bool:
                sql_types[key] = Boolean
            elif py_type is None:
                continue  # We continue here so as to not create a column for null values
            else:
                raise Exception("Error: py_type {} is not an integer, float, datetime,"
                                " none, or string".format(py_types[key]))
        return sql_types

    # Table modification functions
    def dict_to_rows(self):
        """Convert and return an input dictionary into rows compatible with sqlalchemy's insert function

        Currently presumes each dictionary object is a list of equivalent length. Calls _dict_to_rows() to do primary
        processing. Does not yet function with lists.

        Returns:
            a list of rows compatible with sqlalchemy's insert function

        Raise:
            Exception: If the input is neither a list nor dictionary, an exception is raised
        """
        if isinstance(self.data, dict):
            self.rows = self._dict_to_rows()
            return self.rows
        elif isinstance(self.data, list):
            self.rows = self._list_to_rows()
            return self.rows
        else:
            raise Exception("tbl is neither a list or dictionary, and cannot be handled")

    def _dict_to_rows(self):
        """Convert and return an input dictionary into rows compatible with sqlalchemy's insert function"""

        rows = []
        keys = list(self.data.keys())
        # The length of the data should be checked outside the function to ensure each value is an equal length object
        length = len(self.data[keys[0]])
        for i in range(length):
            row_dict = dict()
            for key in keys:
                row_dict[key] = self.data[key][i]
            rows.append(row_dict)
        return rows

    def _list_to_rows(self):
        """Not yet functional

        To-do:
            Implement functionality for transforming lists into database rows"""

        raise Exception("tbl is a list. Function to convert lists into database rows is not implemented")

    def validate_data_length(self):
        """Given a dictionary where keys references lists, check that all lists are the same length, and return T or F

        Returns:
             True: if all the lists in the dictionary have the same length
             False: if the dictionary's lists are of different lengths
        """
        keys = self.data.keys()
        lengths = []
        for key in keys:
            lengths.append(len(self.data[key]))
        length_set = set(lengths)
        if len(length_set) == 1:
            return True
        else:
            return False
