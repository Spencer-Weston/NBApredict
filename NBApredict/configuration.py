"""
Path contains function which return file and folder paths for the project
ToDo: Research os.environ for setting variables. These may still be stored in Config, but they may offer a default
ToDo: or a different way to set variables, particularly for file pathes.
"""
import os
import yaml
from nbapredict.helpers.classes import NestedDict


def project_directory():
    """Returns the project directory so long as configuration.py is in the top-level of the project"""
    return os.path.abspath(os.path.dirname(__file__))


def settings_file():
    """Returns the file path of settings.yaml"""
    return os.path.join(project_directory(), "settings.yaml")


def output_directory():
    """Returns the path to the output folder which holds the database and graphs"""
    return os.path.join(project_directory(), "outputs")


def rreplace(string, old, new, count):
    """Replace old with new in a string in reverse order.
    Args:
        string: String to modify
        old: Sub-string to replace
        new: Sub-string to replace old
        count: The number old sub-strings to be replaced"""
    li = string.rsplit(old, count)
    return new.join(li)


def database_file(calling_file_path):
    """Return the database file path with the path modified in relation to the path the function is called from.

    The base path is r"sqlite:///outputs//nba_db.db". This function modifies that path in relation to the calling file
    path by inserting ..// to the front of the base path. So a file nested one level below the root directory becomes
    r"sqlite:///..//outputs//nba_db.db"
    """
    head_path = project_directory()
    head_folder = os.path.split(head_path)[1]

    if os.path.realpath(calling_file_path) in head_path:
        # If NBApredict is imported from outside the project, replace calling_file_path with head_path
        calling_file_path = head_path

    calling_file_path = calling_file_path.replace("\\", "/")
    #print("Calling_file_path:", calling_file_path)
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
        modified_path = calling_file_path
        for folder in sub_dirs:
            modified_path = rreplace(modified_path, folder, "..", 1)

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


class Configuration:
    """Read and write configuration settings from settings.yaml

    Warning:
        Configuration cannot handle duplicate keys even if keys are of a different depth

    Attributes:
        _file: the source file of the Configuration instance
        _config: a dictionary of settings
        _key_order: each key in _config with values listing keys above the specified key
    """

    def __init__(self, file, settings):
        """sets _config to the settings dictionary and stores the _key_order for accessing each element in _config"""
        self._file = file
        self._key_order = self._generate_config_keys(settings)
        self._config = NestedDict(settings)

    def _generate_config_keys(self, config_dict, path=None, result=None, depth=0, ):
        """Return a dictionary with each key, of any depth, in self._config.

        Each key's value is an ordered list of the nodes above the key and the key itself in self._config. A key in the
        fourth level of config will be: {key: [node1, node2, node3, key]}.

        Args:
            config_dict: A dictionary of configuration options
            path: A list of keys above the current key in the dictionary
            result: A dictionary which stores results
            depth: The current depth of the recursion
        """
        # Initialize path and result. We avoid defaults so path and result are reset on each call
        if path is None:
            path = []
        if result is None:
            result = {}
        for key, value in config_dict.items():
            if depth == 0:  # Reset path each time the function reaches a top-level key in the dictionary
                path = [key]
            if type(value) is dict:
                if key not in path:
                    path.append(key)
                if key not in result.keys():
                    result.update({key: path[:]})  # Create a new list to store path's current state
                result = self._generate_config_keys(value, path, result, depth=depth + 1)
            else:
                result.update({key: path[:]})
                result[key].append(key)

        return result

    def get_property(self, property_key):
        """Return the property associated with the property key from _config.

        Args:
            property_key: The key, of any depth, of the desired property
        """
        if property_key not in self._key_order.keys():
            return None
        elif property_key in self._config.dict.keys():  # Checks if named property is in the top level of _config
            return self._config[property_key]
        else:
            return self._config[self._key_order[property_key]]

    def _set_property(self, property_key, value):
        """Private function for modifying key:value pairs in self._config.

        Additionally, rewrites self._key_order in order to store changes."""
        if property_key not in self._key_order.keys():
            raise KeyError("'{}' not in Config. Manually modify the settings.yaml file if you wish to add new"
                           " settings.".format(property_key))
        keys = [i for i in self._key_order[property_key]]
        self._config[keys] = value
        self._key_order = self._generate_config_keys(self._config.dict)

    def _write(self):
        """Private function for over-writing self._config to the settings file"""


def create_configuration(file, config_settings):
    """Return an instantiated Configuration class."""
    return Configuration(file, config_settings)


def check_paths(config, comp_dict):
    no_match = {}
    for k, v in comp_dict.items():
        if config.get_property(k) != v:
            no_match[k] = v
    return no_match


def set_paths(config, change_dict):
    for k, v in change_dict.items():
        config._set_property(k, v)
    return config


with open(settings_file(), "r") as file:
    config_settings = yaml.safe_load(file)

Config = create_configuration(settings_file(), config_settings)
paths = {"directory": project_directory(), "database": database_file(os.getcwd()), "graph_dir": graphs_directory(),
         "settings": settings_file()}
paths = {"directory": project_directory()}
paths.update({"database": database_file(os.getcwd())})
change_paths = check_paths(Config, paths)
set_paths(Config, change_paths)

# noinspection PyProtectedMember
# Config._set_property("four_factor_regression", "something_else")


t = 2
