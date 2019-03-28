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
    """Return the database file path with the path modified in relation to the path the function is called from
    """
    head_path = project_directory()
    head_folder = os.path.split(head_path)[1]

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
        relative_path_addin = modified_path.split(head_folder)[1]
        relative_path_addin = relative_path_addin.replace("/", "//")
        while relative_path_addin[0] == "/":
            relative_path_addin = relative_path_addin[1:]
        db_path = r"sqlite:///{}//outputs//nba_db.db".format(relative_path_addin)
        return db_path
    else:
        return r"sqlite:///outputs//nba_db.db"


def graphs_directory():
    return os.path.join(output_directory(), "graphs")
