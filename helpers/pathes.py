"""
Author: Spencer Weston
"""

import os


def project_directory():
    """Returns the project directory so long as type.py is in the top-level of the project"""
    return os.path.abspath(os.path.dirname(__file__))


def database_directory():
    return "outputs"


def graphs_directory():
    return "outputs/graphs"
