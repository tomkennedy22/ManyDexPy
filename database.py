import json
import os
from typing import Dict, Any

from table import Table  # Assuming table.py exists with a Table class

class Database:
    def __init__(self, dbname: str, folder_path: str, do_compression: bool):
        self.dbname = dbname
        self.folder_path = folder_path
        self.tables: Dict[str, Table] = {}
        self.storage_location = f"{folder_path}/{dbname}"
        self.output_file_path = f"{self.storage_location}/_{dbname}.json"
        self.do_compression = do_compression

    def add_table(self, table_name: str, indices: list, primary_key: str, proto: Any, delete_key_list: list) -> Table:
        if not table_name:
            raise ValueError("Table name is required")

        if table_name not in self.tables:
            new_table = Table(table_name, indices, self.storage_location, self.dbname, primary_key, proto, delete_key_list, self.do_compression)
            self.tables[table_name] = new_table
            return new_table
        else:
            return self.tables[table_name]

    def add_connection(self, table_a_name: str, table_b_name: str, join_key: str, join_type: str) -> None:
        opposite_join_type = {
            'one_to_many': 'many_to_one',
            'many_to_one': 'one_to_many',
            'one_to_one': 'one_to_one',
        }

        table_a = self.tables.get(table_a_name)
        table_b = self.tables.get(table_b_name)

        if not table_a or not table_b:
            raise ValueError(f"Table does not exist for connection - {table_a_name} or {table_b_name}")

        table_a.table_connections[table_b_name] = {'join_key': join_key, 'join_type': join_type}
        table_b.table_connections[table_a_name] = {'join_key': join_key, 'join_type': opposite_join_type[join_type]}
        print(f'Added connection: {table_a_name}, {table_b_name}, {join_key}, {join_type}')

    async def save_database(self):
        table_info = [{'table_name': table.table_name, 'indices': table.indices, 'primary_key': table.primary_key} for table in self.tables.values()]
        save_data = {
            'dbname': self.dbname,
            'tables': table_info,
            'storage_location': self.storage_location,
            'output_file_path': self.output_file_path,
            'do_compression': self.do_compression,
        }

        data = json.dumps(save_data, indent=2)

        os.makedirs(os.path.dirname(self.output_file_path), exist_ok=True)
        with open(self.output_file_path, 'w') as f:
            f.write(data)

        for table in self.tables.values():
            await table.output_to_file()  # Assuming Table class has an async output_to_file method

    async def read_from_file(self):
        try:
            with open(self.output_file_path, 'r') as f:
                parsed_data = json.load(f)

            self.dbname = parsed_data['dbname']
            self.storage_location = parsed_data['storage_location']
            self.do_compression = parsed_data['do_compression']

            for table_info in parsed_data['tables']:
                table_obj = self.add_table(**table_info, proto=None)
                await table_obj.read_from_file()  # Assuming Table class has an async read_from_file method
        except FileNotFoundError:
            pass  # Handle error or log as needed
