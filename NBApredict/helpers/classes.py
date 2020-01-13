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

    def __keys__(self, depth=0):
        """Does not yet function

        Notes on next steps in the __recurse_keys__ function
        """
        keys = [[k] for k in self.dict.keys()]
        for k in keys:
            branch = self[k[0]]
            self.__recurse_keys__(key=k[0], branch=branch, key_list=k)
            branch = self[k[0]]
            if isinstance(branch, dict):
                if len(branch.keys()) > 1:
                    b_keys = list(branch.keys())
                    length = len(b_keys)
                    # Copies of k to append the keys in the last layer to. When multiple keys are in the last layer,
                    # we need new tress to capture all key paths
                    new_trees = [k for _ in range(length)]
                    k.append(b_keys[0])  # Add the first key to the original tree
                    for i in range(1, length):
                        tree = new_trees[i]
                        tree.append(b_keys[i])
                        keys.append(tree)
                else:
                    k.append(list(branch.keys())[0])
            while not isinstance(branch, dict):
                branch_keys = [[bk] for bk in branch.dict.keys()]

        return keys

    @staticmethod
    def __recurse_keys__(branch, key_list, depth=0):
        """Not Functional

        Waiting to finish this up. There's several issues.
        1. You almost have to recurse through the tree of keys which can be a heavy computation
        2. The leaf of a branch needs to be handled in a different manner than branches along the way. The leaf will
        be a list itself, and it needs to be reformatted when finished.
        3. Finally, it may need a completely different implementation. Look at it with fresh eyes when you next work on
        it. """
        b_keys = list(branch.keys())
        if isinstance(branch, dict):
            if len(b_keys) > 1:
                length = len(b_keys)
                # Copies of k to append the keys in the last layer to. When multiple keys are in the last layer,
                # we need new tress to capture all key paths
                new_trees = [key_list for _ in range(1, length)]
                key_list.append(b_keys[0])  # Add the first key to the original tree
                print(key_list)
                combined_k_lists = [key_list]
                for i in range(length-1):
                    tree = new_trees[i]
                    print(tree)
                    print(b_keys)
                    tree.append(b_keys[i])
                    combined_k_lists.append(tree)
                return combined_k_lists
            else:
                return key_list.append(list(branch.keys())[0])
        else:  # we have a nested dict
            pass