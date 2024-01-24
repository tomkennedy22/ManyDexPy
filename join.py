from typing import Dict, List, Set, Any

def most_precise_query_table(query_addons: Dict[str, Any]) -> str:
    max_key, max_count = None, 0
    for key, value in query_addons.items():
        count = len(value)
        if count > max_count:
            max_key, max_count = key, count
    return max_key

def highest_parent(db, table_names: List[str], query_addons: Dict[str, Any]) -> str:
    tables_without_parent_set = set(table_names)

    for table_name in table_names:
        table = db.tables[table_name]
        for connected_table_name, connection in table.table_connections.items():
            if connection['join_type'] == 'many_to_one' and connected_table_name in table_names:
                tables_without_parent_set.discard(table_name)

    tables_without_parent = list(tables_without_parent_set)
    if not tables_without_parent:
        return most_precise_query_table(query_addons) or table_names[0]
    elif len(tables_without_parent) > 1:
        tables_with_query_addons = [t for t in tables_without_parent if t in query_addons]
        if tables_with_query_addons:
            return tables_with_query_addons[0]
        else:
            raise Exception('Multiple tables without parent found, but none have query addons')
    else:
        return tables_without_parent[0]

def join(db, base_table_name: str, include_table_names: List[str], query_addons: Dict[str, Any] = None):
    join_tracker = {'results': [], 'tables': {}}
    all_tables_needed = set([base_table_name] + include_table_names)

    query_addons = query_addons or {}
    first_table = highest_parent(db, list(all_tables_needed), query_addons)

    join_from_table_results = join_for_table(db, first_table, all_tables_needed, query_addons, join_tracker)

    if first_table != base_table_name:
        all_tables_needed = set([base_table_name] + include_table_names)
        join_from_table_results = join_for_table(db, base_table_name, all_tables_needed, query_addons, join_tracker)

    return join_from_table_results

def join_for_table(db, table_name: str, all_tables_needed: Set[str], query_addons: Dict[str, Any], join_tracker: Dict[str, Any]):
    table = db.tables[table_name]
    all_tables_needed.discard(table_name)
    data = table.find(query_addons.get(table_name, {})) or []

    join_tracker['tables'][table_name] = {
        'data': data,
        'indexes': {key: index_by(data, key) for key in table.get_foreign_keys_and_primary_keys()},
        'groups': {key: group_by(data, key) for key in table.get_foreign_keys_and_primary_keys()}
    }

    for connected_table_name, connection in table.table_connections.items():
        if connected_table_name not in all_tables_needed:
            continue

        join_key = connection['join_key']
        parent_join_ids = distinct([get_from_dict(row, join_key) for row in data])
        child_query = query_addons.get(connected_table_name, {})
        parent_id_query = {join_key: {'$in': parent_join_ids}}
        new_child_query = {**child_query, **parent_id_query}

        join_tracker = join_for_table(db, connected_table_name, all_tables_needed, query_addons, join_tracker)
        child_data_by_key = join_tracker['tables'][connected_table_name]['indexes'][join_key] if connection['join_type'] == 'many_to_one' else join_tracker['tables'][connected_table_name]['groups'][join_key]
        store_key = connected_table_name if connection['join_type'] == 'many_to_one' else f"{connected_table_name}s"

        data = nest_children(data, child_data_by_key, join_key, store_key)

    join_tracker['results'] = join_tracker['tables'][table_name]['data']
    return join_tracker

