"""
Author: Spencer Weston
"""

import os


def project_directory():
    """Returns the project directory so long as type.py is in the top-level of the project"""
    return os.path.abspath(os.path.dirname(__file__))


def output_directory():
    return os.path.join(project_directory(), "outputs")


def database_file(calling_file_path):
    """Return the database file path with the path modified in relation to the path the function is called from.

    The base path is r"sqlite:///outputs//nba_db.db". This function modifies that path in relation to the calling file
    path by inserting ..// to the front of the base path. So a file nested one level below the root directory becomes
    r"sqlite:///..//outputs//nba_db.db"
    """
    head_path = project_directory()
    head_folder = os.path.split(head_path)[1]

    calling_file_path = calling_file_path.replace("\\", "/")
    sub_dirs = []
    split_path = os.path.split(calling_file_path)
    path = split_path[0]
    folder = split_path[1]
    while folder != head_folder:
        sub_dirs.append(folder)
        split_path = os.path.split(path)
        path = split_path[0]
        folder = split_path[1]

    if len(sub_dirs) > 0:
        for folder in sub_dirs:
            modified_path = calling_file_path.replace(folder, "..")
        path_addin = modified_path.split(head_folder)[1]
        path_addin = path_addin.replace("/", "//")
        while path_addin[0] == "/":
            path_addin = path_addin[1:]
        db_path = r"sqlite:///{}//outputs//nba_db.db".format(path_addin)
        return db_path
    else:
        return r"sqlite:///outputs//nba_db.db"


def graphs_directory():
    return os.path.join(output_directory(), "graphs")
