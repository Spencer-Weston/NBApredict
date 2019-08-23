"""
This file contains a DBInterface class which dictates table creation, deletion, and access.
"""

import os
from sqlalchemy import Column, Integer, Table
from sqlalchemy import create_engine, MetaData, event
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import mapper, clear_mappers
from sqlalchemy.ext.automap import automap_base

# Local Imports
from nbapredict.configuration import Config


class DBInterface:
    """DBInterface contains high level information about the desired database and creation, deletion, and access functions

    Attributes:
         path: The path to the database
         engine: SQLalchemy engine for accessing the database
         metadata: Metadata for the engine, used mostly for table access / reflection
         Base: SQLalchemy declarative_base() used for table creation
    """

    class Template(object):
        """Blank template to map tables to with the sqlalchemy mapper function

        Note:
            Template can only be mapped to one table at a time. Use clear_mappers to free the template for new tables
        """
        pass

    def __init__(self, url=None):
        """Initialize macro-level SQLalchemy objects as class attributes (engine, metadata, base).

        A session will allow interaction with the DB."""
        if not url:
            file_path = os.getcwd()
            self.path = Config.get_property("database")
        else:
            self.path = url
        self.engine = create_engine(self.path, pool_pre_ping=True)
        self.metadata = MetaData(self.engine)
        self.Base = declarative_base()

    def get_tables(self, table_names=False):
        """Find and return the specified tables or return all tables.

        Primary use is to check if table exists in database. Use get_table_mappings() for ORM style table interactions
        """
        meta = MetaData(bind=self.engine)
        meta.reflect(bind=self.engine)
        if table_names:
            return meta.tables[table_names]
        else:
            return meta.tables

    def get_table_mappings(self, table_names):
        """Find and return the specified table mappings or return all table mappings

        Args:
         table_names: The table names for which mappings are desired. Either a string or list
        """
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
        """Check if a table exists in the database; Return True if it exists and False otherwise."""
        self.metadata.reflect(bind=self.engine)
        if tbl_name in self.metadata.tables:
            return True
        else:
            return False

    def create_tables(self):
        """Creates all tables which have been made or modified with the Base class of the DBInterface

        Note that existing tables which have been modified, such as by adding a relationship, will be updated when
        create_tables() is called. """
        self.metadata.create_all(self.engine)

    def map_table(self, tbl_name, column_types, constraints=None):
        """Map a table named tbl_name and with column_types to Template, add constraints if specified.

        Note: Foreign key constraints should likely be added to the mapped table explicitly rather than in this function

        Args:
            tbl_name: The name of the table to be mapped
            column_types: A dictionary with column names as keys and sql types as values
            constraints: A dictionary of desired constraints where the constraints (Such as UniqueConstraint) are keys
            and the columns to be constrained is a list of string column names
        """
        columns = self._generate_columns(column_types)
        if constraints:
            t = Table(tbl_name, self.metadata, Column('id', Integer, primary_key=True),
                      *columns,
                      *(constraint(*columns) for constraint, columns in constraints.items()),
                      )
        else:
            t = Table(tbl_name, self.metadata, Column('id', Integer, primary_key=True),
                      *columns
                      )

        mapper(self.Template, t)

    @staticmethod
    def _generate_columns(columns):
        """Take columns where key is the column name and value is the column type into SQLlachemy columns.

        To use additional arguments, such as constraints, specify column values as a list where the constraints are
        elements of the list"""
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

        Uses sqlalchemy's "Classic" method. ORM database interactions are mediated by sessions.
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
    """SQLalchemy listener function to allow foreign keys in SQLite"""
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()
