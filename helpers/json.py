"""
JSON interaction class and functions.

Created for a use-case which is no longer needed. This module is not used in the project.
"""
import copy
import json
import os
import yaml

# Local imports
from helpers import type


class JsonFile:
    """A class to handle JSON functionality such as load, create, add, and drop"""
    def __init__(self, json_file):
        self.path = json_file
        if os.path.isfile(self.path):
            return
        else:  # Create a blank JSON if the file does not already exist
            self.create_json()

    def add_objects(self, objects_dict):
        """Adds a new object or objects to an existing json file

        To-do:
            Currently rewrites the entire file which could be a performance issue. To change, make so that the json file
            endings are removed, a comma inserted, and then re-insert the ending (or something like that)"""

        data = self.load_json()
        modified_data = copy.deepcopy(data)
        try:
            for key, value in objects_dict.items():
                if type.is_python_type(value):  # Creates a yaml representation of python types
                    value = yaml.dump(value)
                modified_data[key] = value
            self.create_json(modified_data)
        except (TypeError, json.decoder.JSONDecodeError):  # Rewrite the initial JSON if an error is encountered
            self.create_json(data)
            raise Exception("Could not add object to JSON. Json restored to previous format")

    def remove_objects(self, keys):
        """Removes the specified object or objects from the json_file as specified by keys"""
        with open(self.path, encoding='utf-8') as data_file:
            data = json.loads(data_file.read())

        changed_data = data
        if isinstance(keys, str):
            del changed_data[keys]
        else:
            for key in keys:
                del changed_data[key]
        try:
            self.create_json(changed_data)
        except TypeError:
            self.create_json(data)

    def create_json(self, object_dict=None):
        """Creates a json to store the specified objects"""
        if object_dict:
            with open(self.path, 'w') as fp:
                json.dump(object_dict, fp, sort_keys=True, indent=4)
        else:
            with open(self.path, 'w') as fp:
                json.dump({}, fp, sort_keys=True, indent=4)

    def check_for_object(self, object_key):
        json_keys = self.load_json().keys()
        if object_key in json_keys:
            return True
        else:
            return False

    def load_json(self):
        with open(self.path, "r") as file:
            python_object = json.load(file)
        return python_object
