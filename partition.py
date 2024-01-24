import json
import os
import asyncio
import gzip
from typing import Any, Dict

class Partition:
    def __init__(self, storage_location: str, partition_indices: Dict[str, Any], primary_key: str, proto, do_compression: bool, partition_name: str = None):
        self.partition_indices = partition_indices
        self.primary_key = primary_key
        self.proto = proto
        self.data = {}
        self.partition_name = partition_name or self._partition_name_from_partition_index(partition_indices)
        self.storage_location = storage_location
        self.json_output_file_path = os.path.join(storage_location, f"{self.partition_name}.json")
        self.txt_output_file_path = os.path.join(storage_location, f"{self.partition_name}.txt")
        self.last_update_dt = None
        self.do_compression = do_compression
        self.is_dirty = True
        self.write_lock = False

    def _partition_name_from_partition_index(self, partition_indices):
        return '_'.join([f'{key}_{value}' for key, value in partition_indices.items()]) or 'default'

    async def write_to_file(self):
        if not self.is_dirty or self.write_lock:
            return

        self.write_lock = True
        try:
            self.is_dirty = False
            output_data = {
                "partition_name": self.partition_name,
                "partition_indices": self.partition_indices,
                "data": self.data,
                "storage_location": self.storage_location,
                "primary_key": self.primary_key,
                "last_update_dt": self.last_update_dt.isoformat() if self.last_update_dt else None
            }
            data = json.dumps(output_data, indent=2)
            output_file_path = self.txt_output_file_path if self.do_compression else self.json_output_file_path

            os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
            if self.do_compression:
                async with gzip.open(output_file_path, 'wt', encoding='utf-8') as f:
                    await f.write(data)
            else:
                with open(output_file_path, 'w') as f:
                    f.write(data)
        except Exception as e:
            print(f"Error writing file: {e}")
            self.is_dirty = True
        finally:
            self.write_lock = False

    async def read_from_file(self):
        output_file_path = self.txt_output_file_path if self.do_compression else self.json_output_file_path
        try:
            if self.do_compression:
                async with gzip.open(output_file_path, 'rt', encoding='utf-8') as f:
                    data = await f.read()
            else:
                with open(output_file_path, 'r') as f:
                    data = f.read()
            parsed_data = json.loads(data)
            self.partition_name = parsed_data.get("partition_name")
            self.partition_indices = parsed_data.get("partition_indices")
            self.data = parsed_data.get("data")
            self.storage_location = parsed_data.get("storage_location")
            self.primary_key = parsed_data.get("primary_key")
            self.last_update_dt = parsed_data.get("last_update_dt")
        except FileNotFoundError:
            print(f"File not found: {output_file_path}")
        except Exception as e:
            print(f"Error reading from file: {e}")

    async def delete_file(self):
        output_file_path = self.txt_output_file_path if self.do_compression else self.json_output_file_path
        try:
            self.data.clear()
            os.remove(output_file_path)
        except FileNotFoundError:
            print(f"File not found: {output_file_path}")
        except Exception as e:
            print(f"Error deleting file: {e}")

    # Methods insert, update, and other utility methods to be added similar to TypeScript version

    def insert(self, data):
        if not isinstance(data, list):
            data = [data]

        for row in data:
            row_pk = self._get_from_dict(row, self.primary_key)
            if row_pk is None:
                raise ValueError(f"Primary key value missing in the data row. Cannot insert into partition. Table {self.partition_name} and primary key {self.primary_key}")
            elif row_pk in self.data:
                raise ValueError(f"Duplicate primary key value: {row_pk} for field {self.primary_key} in partition {self.partition_name}")

            self.data[row_pk] = row
            self.is_dirty = True

        self.last_update_dt = datetime.datetime.now()

    def update(self, row, fields_to_drop=None):
        fields_to_drop = fields_to_drop or []
        row_pk = self._get_from_dict(row, self.primary_key)
        if row_pk not in self.data:
            raise ValueError(f"Row with primary key {row_pk} does not exist in partition {self.partition_name}.")

        for field in fields_to_drop:
            row.pop(field, None)

        self.data[row_pk] = row
        self.is_dirty = True
        self.last_update_dt = datetime.datetime.now()