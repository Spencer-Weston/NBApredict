"""
Author: Spencer Weston

Purpose: Contains general functions. The NBA_bet project uses these functions. However, their utility is not restricted
to the project. The functions, for the most part, deal with type checks, type conversions, and length comparisons
"""

import copy
from datetime import datetime
from enum import Enum
import json
import os
import yaml


def set_type(values):
    """Convert string values to integers or floats if applicable. Otherwise, return strings.

    If the string value has zero length, none is returned

    Args:
        values: A list of values

    Returns:
        The input list of values modified to match their type. String is the default return value. If the values are
        ints or floats, returns the list formatted as a list of ints or floats. Empty values will be replaced with none.

    To-Do:
        1. Add functionality to coerce elements of lists and not just lists
    """
    test_val = values[0]  # Is there a better method than taking a test val?
    if is_int(test_val):
        return _set_type(values, int)
    elif is_float(test_val):
        return _set_type(values, float)
    else:
        values = [x if len(x) > 0 else None for x in values]  # Set empty strings to None
        return values


def _set_type(values, new_type):
    """Transforms a list of values into the specified new type. If the value has zero length, returns none

    Args:
        values: A list of values
        new_type: A type class to modify the list to

    Returns:
        The values list modified to the new_type. If an element is empty, the element is set to None.
        """

    new_vals = []
    for i in values:
        if len(i) > 0:  # Some values may have len(0); we convert them to None to put into sql db
            new_vals.append(new_type(i))
        else:
            new_vals.append(None)
    return new_vals


def get_type(values):
    """Return the type of the values where type is defined as the modal type in the list.

    Args:
        values: A list or value to get the type for.

    Returns:
        The modal type of a list or the type of the element. Can be integer, float, string, datetime, or none

    To-Do:
        Modal type isn't a full proof method. Need to determine a better method.
    """
    if hasattr(values, "__len__") and (type(values) != type):  # Checks if the object is iterable
        val_types = []
        for i in values:
            val_types.append(_get_type(i))
        return max(set(val_types), key=val_types.count)  # The max, set, and key combo returns the modal type
    elif isinstance(values, Enum):  # For enum objects, pass the value to the get_type function (right choice? IDK)
        return _get_type(values.value)
    else:
        return _get_type(values)


def _get_type(val):
    """Return the type of the value if it is a int, float, or datetime. Otherwise, return a string.

    Args:
        val: A value to get the type of
    Returns:
        The type of the value passed into the function if it is an int, float, datetime, or string
    Raise:
        Exception: An exception raised if the val is not int, float, datetime, or string.
    """
    if isinstance(val, int):
        return "integer"
    elif isinstance(val, float):
        return "float"
    elif isinstance(val, datetime.datetime):
        return "datetime"
    elif isinstance(val, str):
        return "string"
    elif isinstance(val, bool):
        return "bool"
    elif val is None:
        return None
    elif is_python_type(val):  # Handles types that are passed explicitly
        return val
    else:
        raise Exception("Val is not an int, float, datetime, string, Bool, or None")


def is_int(x):
    """Return true if X can be coerced to a integer. Otherwise, return false."""
    try:
        int(x)  # Will raise ValueError if '.2'; will not raise error if .2
        return True
    except ValueError:
        return False


def is_float(x):
    """Return true if X can be coerced to a float. Otherwise, return false."""
    try:
        float(x)
        return True
    except ValueError:
        return False


def is_python_type(x):
    if x in [int, float, datetime.datetime, str, bool, None]:
        return True
    else:
        return False


class JsonFile:
    """A class to handle JSON functionality such as load, create, add, and drop"""
    def __init__(self, json_file):
        self.path = json_file
        if os.path.isfile(self.path):
            return
        else:  # Create a blank JSON if the file does not already exist
            self.create_json()

    def add_objects(self, objects_dict):
        """Adds a new object or objects to an existing json file

        To-do:
            Currently rewrites the entire file which could be a performance issue. To change, make so that the json file
            endings are removed, a comma inserted, and then re-insert the ending (or something like that)"""

        data = self.load_json()
        modified_data = copy.deepcopy(data)
        try:
            for key, value in objects_dict.items():
                if is_python_type(value):  # Creates a yaml representation of python types
                    value = yaml.dump(value)
                modified_data[key] = value
            self.create_json(modified_data)
        except (TypeError, json.decoder.JSONDecodeError):  # Rewrite the initial JSON if an error is encountered
            self.create_json(data)
            raise Exception("Could not add object to JSON. Json restored to previous format")

    def remove_objects(self, keys):
        """Removes the specified object or objects from the json_file as specified by keys"""
        with open(self.path, encoding='utf-8') as data_file:
            data = json.loads(data_file.read())

        changed_data = data
        if isinstance(keys, str):
            del changed_data[keys]
        else:
            for key in keys:
                del changed_data[key]
        try:
            self.create_json(changed_data)
        except TypeError:
            self.create_json(data)

    def create_json(self, object_dict=None):
        """Creates a json to store the specified objects"""
        if object_dict:
            with open(self.path, 'w') as fp:
                json.dump(object_dict, fp, sort_keys=True, indent=4)
        else:
            with open(self.path, 'w') as fp:
                json.dump({}, fp, sort_keys=True, indent=4)

    def check_for_object(self, object_key):
        json_keys = self.load_json().keys()
        if object_key in json_keys:
            return True
        else:
            return False

    def load_json(self):
        with open(self.path, "r") as file:
            python_object = json.load(file)
        return python_object


def return_project_directory():
    """Returns the project directory so long as general.py is in the top-level of the project"""
    return os.path.abspath(os.path.dirname(__file__))


