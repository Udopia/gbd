# python packages
import sqlite3
from urllib.error import URLError

from flask import json
# internal packages
from main.gbd_tool import import_data
from main.gbd_tool.database import groups, benchmark_administration, search
from main.gbd_tool.database.db import Database
from main.gbd_tool.hashing.gbd_hash import gbd_hash
from main.gbd_tool.http_client import post_request


# hash a CSV file
def hash_file(path):
    return gbd_hash(path)


# Import CSV file
def import_file(database, path, key, source, target):
    with Database(database) as database:
        import_data.import_csv(database, path, key, source, target)


# Initialize the GBD database. Create benchmark entries in database if path is given, just create a database otherwise.
# With the constructor of a database object the __init__ method in db.py will be called
def init_database(database, path=None):
    with Database(database) as database:
        if path is not None:
            benchmark_administration.remove_benchmarks(database)
            benchmark_administration.register_benchmarks(database, path)
        else:
            Database(database)


# Get information of the whole database
def get_database_info(database):
    with Database(database) as database:
        return {'name': database, 'version': database.get_version(), 'hash-version': database.get_hash_version()}


# Checks weather a group exists in given database object
def check_group_exists(database, name):
    with Database(database) as database:
        if name in groups.reflect(database):
            return True
        else:
            return False


# Adds a group to given database representing for example an attribute of a benchmark
def add_attribute_group(database, name, type, unique):
    with Database(database) as database:
        groups.add(database, name, unique is not None, type, unique)


# Remove group from database
def remove_attribute_group(database, name):
    with Database(database) as database:
        groups.remove(database, name)


# Get all groups which are in the database
def get_all_groups(database):
    with Database(database) as database:
        return groups.reflect(database)


# Delete entries in groups from database, but don't delete the according group
def clear_group(database, name):
    with Database(database) as database:
        groups.remove(database, name)


# Retrieve information about a specific group
def get_group_info(database, name):
    if name is not None:
        with Database(database) as database:
            return {'name': name, 'type': groups.reflect_type(database, name),
                    'uniqueness': groups.reflect_unique(database, name),
                    'default': groups.reflect_default(database, name),
                    'entries': groups.reflect_size(database, name)}
    else:
        raise ValueError('No group given')


# Retrieve all values the given group contains
def get_group_values(database, name):
    if name is not None:
        return query_search(database, '{} like %%%%'.format(name))
    else:
        raise ValueError('No group given')


# Associate hashes with a hash-value in a group
def set_attribute(database, name, value, hash_list, force):
    with Database(database) as database:
        for h in hash_list:
            benchmark_administration.add_tag(database, name, value, h, force)


# Remove association of a hash with a hash-value in a group
def remove_attribute(database, name, value, hash_list):
    with Database(database) as database:
        for h in hash_list:
            benchmark_administration.remove_tag(database, name, value, h)


# Create an union of two hash lists and return resulting hash list
def hash_union(hash_list, other_hash_list):
    return hash_list.update(other_hash_list)


# Create an intersection of two hash lists and return new hash list
def hash_intersection(hash_list, other_hash_list):
    return hash_list.intersection_update(other_hash_list)


# Search for benchmarks which have to pertain to the query semantics. Before searching, some groups must be added and
# filled with hashes and their values for the according group. Returns list of hashes
def query_search(database, query=None):
    with Database(database) as database:
        try:
            hashes = search.find_hashes(database, query)
        except sqlite3.OperationalError:
            raise ValueError("Cannot open database file")
        return hashes


# Send a query search request to a running GBD server
def query_request(host, query, useragent):
    try:
        return set(post_request("{}/query".format(host), {'query': query}, {'User-Agent': useragent}))
    except URLError:
        raise ValueError('Cannot send request to host')


# Resolve hashes against groups. Returns a list of dictionaries (one for each hash) and several entries
# (their values in given groups)
def resolve(database, hash_list, group_list, pattern=None, collapse=False):
    with Database(database) as database:
        result = []
        for h in hash_list:
            out = {'hash': h}
            for name in group_list:
                if not name.startswith("__"):
                    resultset = sorted(search.resolve(database, name, h))
                    resultset = [str(element) for element in resultset]
                if name == 'benchmarks' and pattern is not None:
                    res = [k for k in resultset if pattern in k]
                    resultset = res
                if len(resultset) > 0:
                    if collapse:
                        out.update({'{}'.format(name): resultset[0]})
                    else:
                        out.update({'{}'.format(name): ' '.join(resultset)})
            result.append(out)
        return result


# Send a resolve request to a running GBD server
def resolve_request(host, hash_list, group_list, collapse, pattern, useragent):
    try:
        hash_list = json.dumps(hash_list)
        group_list = json.dumps(group_list)
        return post_request("{}/resolve".format(host), {'hashes': hash_list, 'group': group_list, 'collapse': collapse,
                                                        'pattern': pattern}, {'User-Agent': useragent})
    except URLError:
        raise ValueError('Cannot send request to host')
