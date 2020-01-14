"""Conversion contains functions to grease interoperability between tables. At the moment, this consists of the
values_to_foreign_key function."""

from nbapredict.helpers.classes import NestedDict
import sqlalchemy

def values_to_foreign_key(session, foreign_tbl, foreign_key, foreign_value, child_data):
    """Return values from child data that exist in the foreign_tbl transformed into foreign key values

    Args:
        foreign_tbl: The foreign table mapping child data references
        foreign_key: The name of the column containing foreign key values
        foreign_value: The name of the column containing values to match with child data
        child_data: A list of data with values contained in foreign value

    Returns:
         A list of values from the foreign key column that correspond to child data's relationship to the foreign values
    """
    # past 999 the SQLite backend raises a "too many variables warning". Here, we presume we don't have >999 unique
    # values in child_data. Rather, presume we have < 999 unique values and take a set of the data.
    set_data = set()
    if len(child_data) > 999:
        set_data = set(child_data)
    if type(foreign_tbl) == sqlalchemy.sql.selectable.Alias:
        conversion_dict = _values_to_foreign_key(foreign_tbl, foreign_key, foreign_value, set_data or child_data)
        return [conversion_dict[i] for i in child_data]
    else:
        key_column = [getattr(foreign_tbl, foreign_key)]
        if isinstance(child_data, dict):
            composite_fd = True  # Composite functional dependency, two+ columns required to identify unique key
            value_columns = [getattr(foreign_tbl, val) for val in child_data.keys()]
            keys = list(child_data.keys())
            filters = [value_columns[i].in_(child_data[keys[i]]) for i in range(len(keys))]
        else:
            composite_fd = False
            value_columns = [getattr(foreign_tbl, foreign_value)]
            filters = [value_columns[0].in_(set_data or child_data)]

        rows = session.query(*key_column, *value_columns).distinct().filter(*filters).all()

        if composite_fd:
            nested_conversion_dict = NestedDict()
            for r in rows:
                # multi-valued key with the foreign key as the value
                nested_conversion_dict[[col for col in r[1:]]] = r[0]

            # Generate a list of lists with the values in each row of child data
            # These values form keys for the foreign keys stored in the nested_conversion_dict which is returned
            conversion_keys = []
            l = len(child_data[list(child_data.keys())[0]])
            for i in range(l):
                conversion_keys.append([child_data[k][i] for k in child_data.keys()])
            return [nested_conversion_dict[k] for k in conversion_keys]
        else:
            conversion_dict = {getattr(row, foreign_value): getattr(row, foreign_key) for row in rows}
            return [conversion_dict[i] for i in child_data]


def _values_to_foreign_key(session, foreign_subquery, foreign_key, foreign_value, child_data):
    """Return values from child data that exist in the foreign_subquery transformed into foreign key values

    This function performs the same query as values_to_foreign_key() except it can take a subquery, which has
    different syntax, as input rather than a table. The function presumes child_data has already been modified if
    necessary. NOTE: this does not support multi-column conversions of child_data to foreign key.

    Args:
        foreign_subquery: A subquery which is an Alias class in sqlalchemy. These classes are created when subquery()
        is appended to a sqlalchemy query statement
        foreign_key: The name of the column containing foreign key values
        foreign_value: The name of the column containing values to match with child data
        child_data: A list of data with values contained in foreign value

    Returns:
         A conversion dict that maps child_data to foreign keys
    """
    rows = session.query(getattr(foreign_subquery.c, foreign_key), getattr(foreign_subquery.c, foreign_value)). \
        filter(getattr(foreign_subquery.c, foreign_value).in_(child_data)).all()
    conversion_dict = {getattr(row, foreign_value): getattr(row, foreign_key) for row in rows}
    return conversion_dict
