"""
Author: Spencer Weston

Purpose: This file contains functions for working with databases. It works with SQL lite databases and uses the
sqlalchemy package to construct the functions.
"""

import os
from sqlalchemy import Column, ForeignKey, Integer, Table
from sqlalchemy import create_engine, MetaData, event
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import mapper, clear_mappers
from sqlalchemy.ext.automap import automap_base

# Local Imports
import path


# def create_col_definitions(tbl_name, id_type_dict, foreign_key=False):
#     """Create and return a dictionary of column specifications for a sql_alchemy table.
#
#     Returns a dictionary that begins with __table__name and an integer id followed by columns as specified
#     in the id_type_dict
#
#     Args:
#         tbl_name: name of the table to create column definitions for
#         id_type_dict: dictionary of column id's as keys and sql_alchemy sql types as values
#         foreign_key: To add a foreign key column, pass a string formatted as foreign "table.column"
#     Returns:
#         Column definitions
#     """
#     col_specs = {'__tablename__': '{}'.format(tbl_name),
#                  'id': Column(Integer, primary_key=True)}
#     if foreign_key:
#         col_specs[foreign_key] = Column(foreign_key, Integer, ForeignKey(foreign_key), nullable=False, unique=True)
#     for key in id_type_dict:
#         col_specs[key] = Column(id_type_dict[key])
#
#     return col_specs


class Database:
    """Database is the highest level in a hierarchy of classes which deal with database interactions with sqlalchemy

    Database serves as the hub for high level attributes such as the Base and engine which apply to all tables in the
    database. """

    class Template(object):
        """Blank template to map tables to with the sqlalchemy mapper function"""
        pass

    def __init__(self, url=None):
        """Creates an engine on the specified path, generates a declarative Base class which reflects the engine

        The base is passed to subclasses in order to generate tables. A session will allow interaction with the DB."""
        if not url:
            file_path = os.getcwd()
            self.path = path.database_file(file_path)
        else:
            self.path = url
        self.engine = create_engine(self.path, pool_pre_ping=True)
        self.metadata = MetaData(self.engine)
        self.Base = declarative_base()

    def get_tables(self, table_names=False):
        """Find and return the specified tables or return all tables"""
        meta = MetaData(bind=self.engine)
        meta.reflect(bind=self.engine)
        if table_names:
            return meta.tables[table_names]
        else:
            return meta.tables

    def get_table_mappings(self, table_names):
        """Find and return the specified table mappings or return all table mappings

        Args:
         table_names: The table names for which mappings are desired. Either a string or list"""
        if isinstance(table_names, str):  # Allows a string, rather than list, to be passed to function
            holder = table_names
            table_names = [holder]

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
        """Creates all tables which have been made or modified with the Base class of the Database"""
        self.metadata.create_all(self.engine)

    def map_table(self, tbl_name, column_types, constraints=None, relationships=None):
        """Maps a dictionary keyed on column names with Type values and, optionally, constraints

        Args:
            tbl_name: The name of the table to be mapped
            column_types: A dictionary with column names as keys and sql types as values
            constraints: A dictionary of desired constraints where the constraints (Such as UniqueConstraint) are keys
            and the columns to be constrained is a list of string column names
            relationships: A dictionary of the desired relationships (functionality not yet tested)
        """
        columns = self._generate_columns(column_types)
        if constraints and relationships:
            t = Table(tbl_name, self.metadata, Column('id', Integer, primary_key=True),
                      *columns,
                      *(constraint(*columns) for constraint, columns in constraints.items()),
                      "RELATIONSHIPS"
                      )
        elif constraints:
            t = Table(tbl_name, self.metadata, Column('id', Integer, primary_key=True),
                      *columns,
                      *(constraint(*columns) for constraint, columns in constraints.items())
                      )
        elif relationships:
            t = Table(tbl_name, self.metadata, Column('id', Integer, primary_key=True),
                      *columns,
                      "RELATIONSHIPS"
                      )
        else:
            t = Table(tbl_name, self.metadata, Column('id', Integer, primary_key=True),
                      *columns
                      )

        mapper(self.Template, t)

    @staticmethod
    def _generate_columns(columns):
        column_list = []
        for key, value in columns.items():
            try:
                column_list.append(Column(key, *value))  # Unpacks additional column arguments
            except TypeError:  # if no additional arguments, just make a standard name and type column
                column_list.append(Column(key, value))
        return column_list

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


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()
