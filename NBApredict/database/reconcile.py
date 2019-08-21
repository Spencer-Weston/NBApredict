"""
At the moment, reconcile contains one function which "reconciles" primary and reference tables for a specific column.
"""


def reconcile(ref_tbl, change_tbl, column, ref_key, change_key, session):
    """Compare the specified column over the two tables and change change_tbl values to ref_tbl values

    Note that the change and reference tables must be related by a foreign key.

    Args:
        ref_tbl: The reference table which contains the values to be changed in change_tbl
        change_tbl: The table to be changed with values from reference table
        column: The column to evaluate for changes. Column must be present in both tables.
        ref_key: The key in the reference table to join the tables by
        change_key: The key in the change table to join the tables by
        session: An instance of a sqlalchemy Session class bound to the database's engine

    To-do:
        Figure out how to run with multiple columns
    """
    join_objs = session.query(ref_tbl, change_tbl).join().\
        filter(getattr(ref_tbl, ref_key) == getattr(change_tbl, change_key)).all()

    changed_objs = []
    for obj in join_objs:
        ref_obj = obj[0]
        change_obj = obj[1]
        ref_val = getattr(ref_obj, column)
        change_val = getattr(change_obj, column)
        if ref_val != change_val:
            setattr(change_obj, column, ref_val)
            session.add(change_obj)

    return
