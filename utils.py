from collections import defaultdict
from copy import deepcopy

# Import for handling date if needed, similar to dayjs
import datetime

def partition_name_from_partition_index(partition_index):
    return '_'.join([f'{key}_{value}' for key, value in partition_index.items()]) or 'default'

def distinct(arr):
    result = []
    for item in arr:
        if item not in result:
            result.append(item)
    return result

def get_from_dict(obj, key):
    key_parts = key.split('.')
    current = obj

    for part in key_parts:
        if isinstance(current, dict):
            current = current.get(part)
            if current is None:
                return None
        else:
            return None
    return current

def set_to_dict(container, key, value):
    keys = key.split('.')
    current_container = container

    for i, current_key in enumerate(keys):
        if i == len(keys) - 1:
            if isinstance(current_container, dict):
                current_container[current_key] = value
        else:
            if current_key not in current_container or not isinstance(current_container[current_key], dict):
                current_container[current_key] = {}
            current_container = current_container[current_key]

    return container

def deep_copy(obj, hash=None):
    if hash is None:
        hash = {}
    if obj is None or not isinstance(obj, (dict, list)):
        return obj

    if id(obj) in hash:
        return hash[id(obj)]

    if isinstance(obj, list):
        lst = []
        hash[id(obj)] = lst
        for item in obj:
            lst.append(deep_copy(item, hash))
        return lst

    if isinstance(obj, dict):
        dct = {}
        hash[id(obj)] = dct
        for key, value in obj.items():
            dct[key] = deep_copy(value, hash)
        return dct

def is_deep_equal(obj1, obj2):
    if obj1 is obj2:
        return True

    if not all(isinstance(obj, (dict, list)) for obj in [obj1, obj2]):
        return False

    if isinstance(obj1, list) and isinstance(obj2, list):
        return all(is_deep_equal(x, y) for x, y in zip(obj1, obj2))

    if isinstance(obj1, dict) and isinstance(obj2, dict):
        if obj1.keys() != obj2.keys():
            return False
        return all(is_deep_equal(obj1[k], obj2[k]) for k in obj1.keys())

    return False

def nest_children(parent_array, child_dict, join_key, store_key):
    for parent in parent_array:
        parent[store_key] = child_dict.get(parent[join_key])
    return parent_array

def index_by(list_, index_field):
    index_map = {}
    for row in list_:
        index_value = get_from_dict(row, index_field)
        index_map[index_value] = row
    return index_map

def group_by(list_, index_field):
    group_map = defaultdict(list)
    for row in list_:
        index_value = get_from_dict(row, index_field)
        group_map[index_value].append(row)
    return group_map
