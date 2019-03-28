"""
Author: Spencer Weston
"""

import os


def project_directory():
    """Returns the project directory so long as type.py is in the top-level of the project"""
    return os.path.abspath(os.path.dirname(__file__))


def output_directory():
    return os.path.join(project_directory(), "outputs")


def database_file():
    return r"sqlite:///outputs//nba_db.db"


def graphs_directory():
    return os.path.join(output_directory(), "graphs")
