"""
Author: Spencer Weston

Purpose: Contains general functions. The NBA_bet project uses these functions. However, their utility is not restricted
to the project. The functions, for the most part, deal with type checks, type conversions, and length comparisons
"""

import datetime
from enum import Enum


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
    if hasattr(values, "__len__"):  # Checks if the object is iterable
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
    elif val is None:
        return None
    else:
        raise Exception("Val is not an int, float, datetime, or string")


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


def check_dict_list_equivalence(dict_object):
    """Given a dictionary where keys references lists, check that all lists are the same length, and return True or False

    Args:
        dict_object: a dictionary where each key references a list

    Returns:
         True: if all the lists in the dictionary have the same length
         False: if the dictionary's lists are of different lengths
    """
    keys = dict_object.keys()
    lengths = []
    for key in keys:
        lengths.append(len(dict_object[key]))
    length_set = set(lengths)
    if len(length_set) == 1:
        return True
    else:
        return False
