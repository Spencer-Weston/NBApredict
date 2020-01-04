"""Generic classes used throughout the project"""


class NestedDict:
    """NestedDict allows multi-level dictionaries which """
    def __init__(self, *args, **kwargs):
        """Creates a standard dictionary as a class property"""
        self.dict = dict(*args, **kwargs)

    def __getitem__(self, keys):
        """Returns the value for key and accepts iterables as keys to reach lower level branches of the dict."""
        # Allows getting top-level branch when a single key was provided
        if not isinstance(keys, tuple):
            if isinstance(keys, str) or isinstance(keys, int):  # Handles single item lists or strings
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
                keys = (*keys,)
            else:
                keys = tuple(keys)

        branch = self.dict
        for key in keys[:-1]:
            if key not in branch:
                branch[key] = {}
            branch = branch[key]
        branch[keys[-1]] = value
