import json
import os
import asyncio
from typing import Any, Dict, List, Optional

# Assuming partition.py and results.py exist with Partition and Results classes respectively
from partition import Partition
from results import Results
from utils import get_from_dict, partition_name_from_partition_index

class Table:
    def __init__(self, table_name: str, indices: List[str], storage_location: str, primary_key: str, proto: Any, delete_key_list: List[str], do_compression: bool):
        self.table_name = table_name
        self.indices = indices or []
        self.primary_key = primary_key
        self.proto = proto
        self.partitions_by_partition_name: Dict[str, Partition] = {}
        self.partition_name_by_primary_key: Dict[str, str] = {}
        self.delete_key_list = delete_key_list or []
        self.do_compression = do_compression or False
        self.storage_location = f"{storage_location}/{table_name}"
        self.output_file_path = f"{self.storage_location}/_{table_name}.json"
        self.table_connections: Dict[str, Dict[str, str]] = {}

    async def output_to_file(self):
        try:
            partitions = self.find_partitions()
            output_data = {
                "table_name": self.table_name,
                "indices": self.indices,
                "primary_key": self.primary_key,
                "partition_names": list(self.partitions_by_partition_name.keys()),
                "output_file_path": self.output_file_path,
                "storage_location": self.storage_location,
                "table_connections": self.table_connections,
                "do_compression": self.do_compression,
            }
            data = json.dumps(output_data, indent=2)

            os.makedirs(os.path.dirname(self.output_file_path), exist_ok=True)

            write_promises = [partition.write_to_file() for partition in partitions]  # Assuming Partition has write_to_file method
            await asyncio.gather(*write_promises)

            with open(self.output_file_path, 'w') as f:
                f.write(data)
        except Exception as error:
            print(f"Error in output_to_file: {error}")

    async def read_from_file(self):
        try:
            with open(self.output_file_path, 'r') as f:
                parsed_data = json.load(f)

            self.table_name = parsed_data['table_name']
            self.indices = parsed_data['indices']
            self.primary_key = parsed_data['primary_key']
            self.storage_location = parsed_data['storage_location']
            self.table_connections = parsed_data['table_connections']
            self.do_compression = parsed_data['do_compression']

            partition_read_promises = [self._read_partition(partition_name) for partition_name in parsed_data['partition_names']]  # Assuming _read_partition method
            await asyncio.gather(*partition_read_promises)
        except FileNotFoundError:
            pass  # Handle error or log as needed

    async def _read_partition(self, partition_name: str):
        partition = Partition(...)  # Instantiate Partition with appropriate arguments
        await partition.read_from_file()  # Assuming Partition has read_from_file method
        self.partitions_by_partition_name[partition_name] = partition
        # Additional logic to update partition_name_by_primary_key

    # Further methods like insert, update, delete, clear, etc., need to be defined here
    # These methods will include the logic similar to the TypeScript version

    def insert(self, data):
        if not isinstance(data, list):
            data = [data]

        data = self.cleanse_before_alter(data)

        for row in data:
            row_pk = get_from_dict(row, self.primary_key)
            partition_indices = {index_name: get_from_dict(row, index_name) for index_name in self.indices}
            partition_name = partition_name_from_partition_index(partition_indices)
            self.partition_name_by_primary_key[row_pk] = partition_name

            if partition_name not in self.partitions_by_partition_name:
                self.partitions_by_partition_name[partition_name] = Partition(...)  # Instantiate with appropriate arguments

            self.partitions_by_partition_name[partition_name].insert(row)  # Assuming Partition has an insert method

    def update(self, data):
        if not isinstance(data, list):
            data = [data]

        data = self.cleanse_before_alter(data)

        for row in data:
            row_pk = get_from_dict(row, self.primary_key)
            existing_partition_name = self.partition_name_by_primary_key.get(row_pk)
            if not existing_partition_name:
                raise ValueError(f"Row with primary key {row_pk} does not exist and cannot be updated.")

            self.partitions_by_partition_name[existing_partition_name].is_dirty = True
            del self.partitions_by_partition_name[existing_partition_name].data[row_pk]
            del self.partition_name_by_primary_key[row_pk]

            self.insert(row)

    def cleanse_before_alter(self, data):
        new_list = []
        for item in data:
            new_item = item.copy()  # Assuming item is a dictionary
            for delete_key in self.delete_key_list:
                new_item.pop(delete_key, None)
            new_list.append(new_item)
        return new_list

    async def delete(self, query=None):
        if not query:
            await self.clear()
            return

        rows = self.find(query)
        for row in rows:
            row_pk = get_from_dict(row, self.primary_key)
            partition_name = self.partition_name_by_primary_key[row_pk]
            partition = self.partitions_by_partition_name[partition_name]
            partition.is_dirty = True
            del partition.data[row_pk]
            del self.partition_name_by_primary_key[row_pk]

    async def clear(self):
        for name, partition in self.partitions_by_partition_name.items():
            await partition.delete_file()  # Assuming Partition has a delete_file method
        self.partitions_by_partition_name.clear()
        self.partition_name_by_primary_key.clear()
    def find_partitions(self):
        return list(self.partitions_by_partition_name.values())

    def normalize_query(self, query):
        normalized_query = {}
        for query_field, query_clause in query.items():
            if isinstance(query_clause, (str, int, bool)):
                normalized_query[query_field] = {'$eq': query_clause}
            else:
                normalized_query[query_field] = query_clause
        return normalized_query

    def filter(self, data, query_field, query_clause):
        if not query_clause or not query_clause.keys():
            return data
        filtered_data = []
        for row in data:
            if all(self.meets_query_condition(get_from_dict(row, query_field), func, value) for func, value in query_clause.items()):
                filtered_data.append(row)
        return filtered_data

    def meets_query_condition(self, field_value, query_function, query_value):
        if query_function == '$eq':
            return field_value == query_value
        elif query_function == '$ne':
            return field_value != query_value
        elif query_function == '$gt':
            return field_value > query_value
        elif query_function == '$gte':
            return field_value >= query_value
        elif query_function == '$lt':
            return field_value < query_value
        elif query_function == '$lte':
            return field_value <= query_value
        elif query_function == '$in':
            return field_value in query_value
        elif query_function == '$nin':
            return field_value not in query_value
        elif query_function == '$between':
            return query_value[0] <= field_value <= query_value[1]
        else:
            raise ValueError(f"Unsupported query function: {query_function}")

    def find(self, input_query=None):
        if not input_query:
            return Results([row for partition in self.partitions_by_partition_name.values() for row in partition.data.values()])

        query = self.normalize_query(input_query)
        valid_partitions = self.find_partitions()

        if query.get(self.primary_key):
            valid_partitions = self.primary_key_partition_filter(valid_partitions, query.pop(self.primary_key))

        for index_name in self.indices:
            if query.get(index_name):
                valid_partitions = self.index_partition_filter(valid_partitions, index_name, query.pop(index_name))

        rows = [row for partition in valid_partitions for row in partition.data.values()]
        for query_key, query_clause in query.items():
            rows = self.filter(rows, query_key, query_clause)

        return Results(rows)

    # def primary_key_partition_filter(self, partitions, query_clause):
        # Implementation of primary key based partition filtering
        # Similar to TypeScript version, but using Python data structures

    # def index_partition_filter(self, partitions, index_name, query_clause):
        # Implementation of index based partition filtering
        # Similar to TypeScript version, but using Python data structures

    def findOne(self, query=None):
        results = self.find(query)
        return results[0] if results else None

    def get_table_connection(self, foreign_table_name):
        return self.table_connections.get(foreign_table_name)

    def get_all_foreign_keys(self):
        return [connection['join_key'] for connection in self.table_connections.values()]

    def get_foreign_keys_and_primary_keys(self):
        return list(set(self.get_all_foreign_keys() + [self.primary_key]))