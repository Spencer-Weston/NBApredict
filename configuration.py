"""
Path contains function which return file and folder paths for the project
"""
import os
from configparser import ConfigParser


def create_config(config_file=None):
    parser = ConfigParser()
    parser.read(config_file)
    return parser


class ConfigTest:
    def __init__(self):
        pass



class Configuration:
    """Read and write configuration settings from settings.ini"""

    def __init__(self, settings):
        """sets _config to the dictionary returned from reading a .yaml document"""
        self._config = settings

    def generate_config_keys(self, key):
        pass

    def get_property(self, property_name):
        if property_name not in self._config.keys():
            return None
        return self._config[property_name]


def project_directory():
    """Returns the project directory so long as configuration.py is in the top-level of the project"""
    return os.path.abspath(os.path.dirname(__file__))


def output_directory():
    """Returns the path to the output folder which holds the database and graphs"""
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
    """Return the folder which holds graphs for the project."""
    return os.path.join(output_directory(), "graphs")
