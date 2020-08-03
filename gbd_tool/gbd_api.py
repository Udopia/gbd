# Global Benchmark Database (GBD)
# Copyright (C) 2019 Markus Iser, Luca Springer, Karlsruhe Institute of Technology (KIT)
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# python packages
import sqlite3
import multiprocessing

from urllib.error import URLError

# internal packages
from gbd_tool import groups, benchmark_administration, search, bootstrap, sanitize
from gbd_tool.db import Database
from gbd_tool.gbd_hash import gbd_hash
from gbd_tool.util import eprint


class GbdApi:
    # Create a new GbdApi object which operates on the given databases
    def __init__(self, db_string, jobs=1, separator=" ", inner_separator=",", join_type="INNER"):
        self.databases = db_string.split(":")
        self.jobs = jobs
        self.mutex = multiprocessing.Lock()
        self.separator = separator
        self.inner_separator = inner_separator
        self.join_type = join_type

    # Calculate GBD hash
    @staticmethod
    def hash_file(path):
        return gbd_hash(path)

    # Import data from CSV file
    def import_file(self, path, key, source, target):
        with Database(self.databases) as database:
            benchmark_administration.import_csv(database, path, key, source, target, self.separator)

    # Initialize table 'local' with instances found under given path
    def init_database(self, path=None):
        eprint('Initializing local path entries {} using {} cores'.format(path, self.jobs))
        if self.jobs == 1 and multiprocessing.cpu_count() > 1:
            eprint("Activate parallel initialization using --jobs={}".format(multiprocessing.cpu_count()))
        with Database(self.databases) as database:
            benchmark_administration.remove_benchmarks(database)
            benchmark_administration.register_benchmarks(self, database, path, self.jobs)

    def bootstrap(self, named_algo):
        with Database(self.databases) as database:
            bootstrap.bootstrap(self, database, named_algo, self.jobs)

    def sanitize(self, hashes):
        with Database(self.databases) as database:
            sanitize.sanitize(self, database, hashes, self.jobs)

    def get_databases(self):
        return self.databases

    # Get all features (or those of given db)
    def get_features(self, path=None):
        if path == None:
            with Database(self.databases) as database:
                return database.tables_and_views()
        elif path in self.databases:
            with Database(path) as database:
                return database.tables_and_views()
        else:
            return []

    # Get all material features (or those of given db)
    def get_material_features(self, path=None):
        if path == None:
            with Database(self.databases) as database:
                return database.tables()
        elif path in self.databases:
            with Database(path) as database:
                return database.tables()
        else:
            return []

    # Get all virtual features (or those of given db)
    def get_virtual_features(self, path=None):
        if path == None:
            with Database(self.databases) as database:
                return database.views()
        elif path in self.databases:
            with Database(path) as database:
                return database.views()
        else:
            return []

    # Check for existence of given feature
    def feature_exists(self, name):
        with Database(self.databases) as database:
            return name in database.tables()

    # Creates the given feature
    def create_feature(self, name, default_value):
        with Database(self.databases) as database:
            groups.add(database, name, default_value is not None, default_value)

    # Removes the given feature
    def remove_feature(self, name):
        with Database(self.databases) as database:
            groups.remove(database, name)

    # Retrieve information about a specific feature
    def get_feature_info(self, name):
        if not name in self.get_features():
            raise ValueError("Attribute '{}' is not available".format(name))
        with Database(self.databases) as database:
            values = database.table_values(name)
            return {'name': name, 
                    'unique': database.table_unique(name),
                    'default-value': database.table_default_value(name),
                    'num-entries': database.table_size(name),
                    'numeric-min': values['numeric'][0], 
                    'numeric-max': values['numeric'][1], 
                    'non-numeric': " ".join(values['discrete']) }

    # Retrieve all values the given feature contains
    def get_feature_values(self, name):        
        if not name in self.get_features():
            raise ValueError("Attribute '{}' is not available".format(name))
        return self.query_search(None, [name], False)

    def callback_set_attributes_locked(self, arg):
        self.set_attributes_locked(arg['hashvalue'], arg['attributes'])

    def set_attributes_locked(self, hash, attributes):
        self.mutex.acquire()
        try:
            # create new connection from old one due to limitations of multi-threaded use (cursor initialization issue)
            with Database(self.databases) as database:
                for attr in attributes:
                    cmd, name, value = attr[0], attr[1], attr[2]
                    database.submit('{} INTO {} (hash, value) VALUES ("{}", "{}")'.format(cmd, name, hash, value))
        finally:
            self.mutex.release()

    # Set the attribute value for the given hashes
    def set_attribute(self, feature, value, hash_list, force):
        if not feature in self.get_material_features():
            raise ValueError("Attribute '{}' is not available (or virtual)".format(feature))
        with Database(self.databases) as database:
            print("Setting {} to {} for benchmarks {}".format(feature, value, hash_list))
            for h in hash_list:
                benchmark_administration.add_tag(database, feature, value, h, force)

    # Remove the attribute value for the given hashes
    def remove_attribute(self, feature, value, hash_list):
        if not feature in self.get_material_features():
            raise ValueError("Attribute '{}' is not available (or virtual)".format(feature))
        with Database(self.databases) as database:
            for h in hash_list:
                benchmark_administration.remove_tag(database, feature, value, h)

    def search(self, feature, hashvalue):
        if not feature in self.get_features():
            raise ValueError("Attribute '{}' is not available".format(feature))
        with Database(self.databases) as database:
            return database.value_query("SELECT value FROM {} WHERE hash = '{}'".format(feature, hashvalue))

    def hash_search(self, hashes=[], resolve=[], collapse=False, group_by=None):
        with Database(self.databases) as database:
            try:
                return search.find_hashes(database, None, resolve, collapse, group_by, hashes, self.inner_separator, self.join_type)
            except sqlite3.OperationalError as err:
                raise ValueError("Query error for database '{}': {}".format(self.databases, err))

    def query_search(self, query=None, resolve=[], collapse=False, group_by=None):
        with Database(self.databases) as database:
            try:
                return search.find_hashes(database, query, resolve, collapse, group_by, [], self.inner_separator, self.join_type)
            except sqlite3.OperationalError as err:
                raise ValueError("Query error for database '{}': {}".format(self.databases, err))
