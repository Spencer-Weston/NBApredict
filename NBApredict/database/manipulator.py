"""
manipulator holds the DataManipulator class which coerces raw_data into SQLalchemy compatible formats.
"""
from datetime import datetime
from nbapredict.helpers import type
from sqlalchemy import Integer, Float, String, DateTime, Boolean


class DataManipulator:
    """DataManipulator takes scraped data in init, and uses its member functions to return manipulations of that data"""

    def __init__(self, data):
        """Stores the data dictionary passed to it

        Args:
            data: A dictionary of data which will, usually, reflect data scraped from a website. Two dictionary
            formats are accepted. First, data may hold column names with data values formatted as:
            data[col1] = [val1, val2, ...]
            data[col2] = [val1, val2, ...]
            Second, data may be a list of rows formatted as:
            data[0] = {col1: val0, col2: val0, colx: val0}
            data[x] = {col1: valx, col2: valx, colx: valx}
        """
        self.data = data
        self.rows = None

    def get_sql_type(self):
        """Take the object's data and return a dictionary formatted as {key: SQLtype}.

        Returns:
            A dictionary with the same keys as tbl_dict. The dictionary's values are the sql_types of each key:value
            pair in tbl_dict. The sql_types are defined to function with SQLalchemy as column definitions.
        """
        py_types = self._get_py_type()  # py_types is a dict
        sql_types = self._py_type_to_sql_type(py_types)
        return sql_types

    def _get_py_type(self):
        """Take the classes data values and return a dictionary that holds the python type for the values.

        Returns:
            A dictionary formatted as key:py_type where the type can be integer, float, string, datetime, or none
        """
        py_types_dict = {}
        if isinstance(self.data, dict):
            tbl_keys = list(self.data.keys())
            py_types = [type.get_type(self.data[key]) for key in tbl_keys]
            py_types_dict = dict(zip(tbl_keys, py_types))
        elif isinstance(self.data, list):
            if isinstance(self.data[0], dict):
                data = self.data[0]
                tbl_keys = list(data.keys())
                py_types = [type.get_type(data[key]) for key in tbl_keys]
                py_types_dict = dict(zip(tbl_keys, py_types))
            else:
                raise Exception("The data structure ({}) is not handled by _get_py_type".format(type(self.data)))
        return py_types_dict

    @staticmethod
    def _py_type_to_sql_type(py_types):
        """Convert and return a dictionary of python types to a dictionary of sql types.

        Raises:
            An exception if a py_type is not an integer, float, string, datetime, bool, or none

        To-do:
            * Change the logic into a switch statement
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
            elif py_type == "datetime" or py_type is datetime:
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
        """Convert and return class data into rows compatible with sqlalchemy's insert function

        Currently presumes each dictionary object is a list of equivalent length. Calls _dict_to_rows() to do primary
        processing. Does not yet function with lists.

        Returns:
            a list of rows compatible with SQLalchemy's

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
        """Convert and return an input dictionary into rows compatible with SQLalchemy"""

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
