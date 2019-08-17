"""
Path contains function which return file and folder paths for the project
"""
import os
import yaml


def project_directory():
    """Returns the project directory so long as configuration.py is in the top-level of the project"""
    return os.path.abspath(os.path.dirname(__file__))


def settings_file():
    """Returns the file path of settings.yaml"""
    return os.path.join(project_directory(), "settings.yaml")


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


class Configuration:
    """Read and write configuration settings from settings.yaml

    Attributes:
        _config: a dictionary of settings
        _key_order: each key in _config with values listing keys above the specified key
    """

    def __init__(self, file, settings):
        """sets _config to the settings dictionary and stores the _key_order for accessing each element in _config"""
        self._file = file
        self._key_order = self._generate_config_keys(settings)
        self._config = NestedDict(settings)

    def _generate_config_keys(self, config_dict, path=[], result={}, depth=0, ):
        """Return a dictionary with each key, of any depth, in self._raw_config.

        Each key's value is an ordered list of the keys/nodes above the key in self._config. A key in the fourth level
        of config [0,1,2,3] will be: {key: [node1, node2, node3]}

        Args:
            config_dict: A dictionary of configuration options
            path: A list of keys above the current key in the dictionary
            result: A dictionary which store results
            depth: The current depth of the recursion
        """
        for key, value in config_dict.items():
            if depth == 0:  # Reset path each time the function reaches a top-level key in the dictionary
                path = []
            if type(value) is dict:
                if key not in result.keys():
                    result.update({key: path[:]})  # Create a new list to store path's current state
                path.append(key)
                result = self._generate_config_keys(value, path, result, depth=depth + 1)
            else:
                result.update({key: path[:]})

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
        """Private function for modifying key:value pairs in self._config"""
        keys = self._key_order[property_key]
        self._config[keys] = value
        #self._key_order = self._generate_config_keys(self._config.dict)

    def _write(self):
        """Private function for over-writing self._config to the settings file"""


def create_configuration(file, config_settings):
    """Return an instantiated Configuration class."""
    return Configuration(file, config_settings)


class NestedDict:
    def __init__(self, *args, **kwargs):
        self.dict = dict(*args, **kwargs)

    def __getitem__(self, keys):
        # Allows getting top-level branch when a single key was provided
        if not isinstance(keys, tuple):
            if isinstance(keys, str):  # Handles single item lists or strings
                keys = (keys,)
            else:
                keys = tuple(keys)

        branch = self.dict
        for key in keys:
            branch = branch[key]

        # If we return a branch, and not a leaf value, we wrap it into a NestedDict
        return NestedDict(branch).dict if isinstance(branch, dict) else branch

    def __setitem__(self, keys, value):
        # Allows setting top-level item when a single key was provided
        if not isinstance(keys, tuple):
            if len(keys) < 2:
                keys = (keys,)
            else:
                keys = tuple(keys)

        branch = self.dict
        for key in keys[:-1]:
            if not key in branch:
                branch[key] = {}
            branch = branch[key]
        branch[keys[-1]] = value


with open(settings_file(), "r") as file:
    config_settings = yaml.load(file)

Config = create_configuration(settings_file(), config_settings)

# noinspection PyProtectedMember
Config._set_property("four_factor_regression", "something_else")

test = Config.get_property("models")
test2 = Config.get_property("four_factor_regression")
test3 = Config.get_property("league_year")
test4 = Config.get_property("database")

t = 2
