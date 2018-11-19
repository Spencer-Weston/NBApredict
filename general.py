import datetime

"""General functions for the project"""


def set_type(values):
    """Converts string values to integers or floats if applicable. Otherwise, returns strings.
    If the string value has zero length, none is returned """

    test_val = values[0]  # Is there a better method than taking a test val?
    if is_int(test_val):
        return _set_type(values, int)
    elif is_float(test_val):
        return _set_type(values, float)
    else:
        values = [x if len(x) > 0 else None for x in values]  # Set empty strings to None
        return values


def get_type(values):
    """Returns the type of the values in a list where type is defined as the modal type in the list"""
    if hasattr(values, "__len__"):
        val_types = []
        for i in values:
            val_types.append(_get_type(i))
        return max(set(val_types), key=val_types.count)  # The max, set, and key combo returns the modal type
    else:
        return _get_type(values)


def _get_type(val):
    """Returns the type of the value if it is a int, float, or datetime. Otherwise, returns strings"""

    if isinstance(val, int):
        return "integer"
    elif isinstance(val, float):
        return "float"
    elif isinstance(val, datetime.datetime):
        return "datetime"
    else:
        return "string"


def is_int(x):
    """Returns true if X can be coerced to a integer. Otherwise, returns false."""

    try:
        int(x)  # Will raise ValueError if '.2'; will not raise error if .2
        return True
    except ValueError:
        return False


def is_float(x):
    """Returns true if X can be coerced to a float. Otherwise, returns false."""
    try:
        float(x)
        return True
    except ValueError:
        return False


def _set_type(values, new_type):
    """Transforms a list of values into the specified new type. If the value has zero length, returns none"""

    new_vals = []
    for i in values:
        # print("value({}) is being set to type({})".format(i, new_type))
        if len(i) > 0:  # Some values may have len(0); we convert them to None to put into sql db
            new_vals.append(new_type(i))
        else:
            new_vals.append(None)
    return new_vals


def check_dict_list_equivalence(dict_object):
    keys = dict_object.keys()
    lengths=[]
    for key in keys:
        lengths.append(len(dict_object[key]))
    length_set = set(lengths)
    if len(length_set) == 1:
        return True
    else:
        return False
