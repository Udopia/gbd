# python packages
import sqlite3
from urllib.error import URLError

from flask import json
# internal packages
from gbd_tool import import_data
from gbd_tool.config_manager import ConfigManager
from gbd_tool.database import groups, benchmark_administration, search
from gbd_tool.database.db import Database
from gbd_tool.hashing.gbd_hash import gbd_hash
from gbd_tool.http_client import post_request


class GbdApi:
    # create a new GbdApi object which operates on a database. The file for this database is parameterized in the
    # constructor and cannot be changed
    def __init__(self, config_path, database):
        self.config_manager = ConfigManager(config_path, database)
        self.database = self.config_manager.get_database_path()

    # hash a CSV file
    @staticmethod
    def hash_file(path):
        return gbd_hash(path)

    # Import CSV file
    def import_file(self, path, key, source, target):
        with Database(self.database) as database:
            import_data.import_csv(database, path, key, source, target)

    # Initialize the GBD database. Create benchmark entries in database if path is given, just create a database
    # otherwise. With the constructor of a database object the __init__ method in db.py will be called
    def init_database(self, path=None):
        if path is not None:
            benchmark_administration.remove_benchmarks(self.database)
            benchmark_administration.register_benchmarks(self.database, path)
        else:
            Database(self.database)

    # Get information of the whole database
    def get_database_info(self):
        with Database(self.database) as database:
            return {'name': database, 'version': database.get_version(), 'hash-version': database.get_hash_version()}

    # Checks weather a group exists in given database object
    def check_group_exists(self, name):
        with Database(self.database) as database:
            if name in groups.reflect(database):
                return True
            else:
                return False

    # Adds a group to given database representing for example an attribute of a benchmark
    def add_attribute_group(self, name, type, unique):
        with Database(self.database) as database:
            groups.add(database, name, unique is not None, type, unique)

    # Remove group from database
    def remove_attribute_group(self, name):
        with Database(self.database) as database:
            groups.remove(database, name)

    # Get all groups which are in the database
    def get_all_groups(self):
        print(self.database)
        with Database(self.database) as database:
            return groups.reflect(database)

    # Delete entries in groups from database, but don't delete the according group
    def clear_group(self, name):
        with Database(self.database) as database:
            groups.remove(database, name)

    # Retrieve information about a specific group
    def get_group_info(self, name):
        if name is not None:
            with Database(self.database) as database:
                return {'name': name, 'type': groups.reflect_type(database, name),
                        'uniqueness': groups.reflect_unique(database, name),
                        'default': groups.reflect_default(database, name),
                        'entries': groups.reflect_size(database, name)}
        else:
            raise ValueError('No group given')

    # Retrieve all values the given group contains
    def get_group_values(self, name):
        if name is not None:
            return self.query_search('{} like %%%%'.format(name))
        else:
            raise ValueError('No group given')

    # Associate hashes with a hash-value in a group
    def set_attribute(self, name, value, hash_list, force):
        with Database(self.database) as database:
            for h in hash_list:
                benchmark_administration.add_tag(database, name, value, h, force)

    # Remove association of a hash with a hash-value in a group
    def remove_attribute(self, name, value, hash_list):
        with Database(self.database) as database:
            for h in hash_list:
                benchmark_administration.remove_tag(database, name, value, h)

    # Create an union of two hash lists and return resulting hash list
    @staticmethod
    def hash_union(hash_list, other_hash_list):
        return hash_list.update(other_hash_list)

    # Create an intersection of two hash lists and return new hash list
    @staticmethod
    def hash_intersection(hash_list, other_hash_list):
        return hash_list.intersection_update(other_hash_list)

    # Search for benchmarks which have to pertain to the query semantics. Before searching, some groups must be added
    # and filled with hashes and their values for the according group. Returns list of hashes
    def query_search(self, query=None):
        with Database(self.database) as database:
            try:
                hashes = search.find_hashes(database, query)
            except sqlite3.OperationalError:
                raise ValueError("Cannot open database file")
            return hashes

    # Send a query search request to a running GBD server
    @staticmethod
    def query_request(host, query, useragent):
        try:
            return set(post_request("{}/query".format(host), {'query': query}, {'User-Agent': useragent}))
        except URLError:
            raise ValueError('Cannot send request to host')

    # Resolve hashes against groups. Returns a list of dictionaries (one for each hash) and several entries
    # (their values in given groups)
    def resolve(self, hash_list, group_list, pattern=None, collapse=False):
        with Database(self.database) as database:
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
    @staticmethod
    def resolve_request(host, hash_list, group_list, collapse, pattern, useragent):
        try:
            hash_list = json.dumps(hash_list)
            group_list = json.dumps(group_list)
            return post_request("{}/resolve".format(host), {'hashes': hash_list, 'group': group_list, 'collapse': collapse,
                                                            'pattern': pattern}, {'User-Agent': useragent})
        except URLError:
            raise ValueError('Cannot send request to host')
