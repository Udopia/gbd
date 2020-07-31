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
    # create a new GbdApi object which operates on a database. The file for this database is parameterized in the
    # constructor and cannot be changed
    def __init__(self, database, jobs=1, separator=" ", inner_separator=",", join_type="INNER"):
        self.database = database
        self.jobs = jobs
        self.mutex = multiprocessing.Lock()
        self.separator = separator
        self.inner_separator = inner_separator
        self.join_type = join_type

    # hash a CNF file
    @staticmethod
    def hash_file(path):
        return gbd_hash(path)

    # Import CSV file
    def import_file(self, path, key, source, target):
        with Database(self.database) as database:
            benchmark_administration.import_csv(database, path, key, source, target, self.separator)

    # Initialize the GBD database. Create benchmark entries in database if path is given, just create a database
    # otherwise. With the constructor of a database object the __init__ method in db.py will be called
    def init_database(self, path=None):
        eprint('Initializing local path entries {} using {} cores'.format(path, self.jobs))
        if self.jobs == 1 and multiprocessing.cpu_count() > 1:
            eprint("Activate parallel initialization using --jobs={}".format(multiprocessing.cpu_count()))
        with Database(self.database) as database:
            benchmark_administration.remove_benchmarks(database)
            benchmark_administration.register_benchmarks(self, database, path, self.jobs)

    def bootstrap(self, named_algo):
        with Database(self.database) as database:
            bootstrap.bootstrap(self, database, named_algo, self.jobs)

    def sanitize(self, hashes):
        with Database(self.database) as database:
            sanitize.sanitize(self, database, hashes, self.jobs)

    # Checks weather a group exists in given database object
    def feature_exists(self, name):
        with Database(self.database) as database:
            return name in database.tables()

    # Adds a group to given database representing for example an attribute of a benchmark
    def create_feature(self, name, default_value):
        with Database(self.database) as database:
            groups.add(database, name, default_value is not None, default_value)

    # Remove group from database
    def remove_feature(self, name):
        with Database(self.database) as database:
            groups.remove(database, name)

    # Get all groups which are in the database
    def get_features(self):
        with Database(self.database) as database:
            return database.tables()

    # Retrieve information about a specific group
    def get_feature_info(self, attribute):
        if not attribute in self.get_features():
            raise ValueError("Attribute '{}' is not available".format(attribute))
        with Database(self.database) as database:
            return {'name': attribute, 
                    'uniqueness': database.table_unique(attribute),
                    'default': database.table_default_value(attribute),
                    'entries': database.table_size(attribute)}

    # Retrieve all values the given group contains
    def get_feature_values(self, attribute):        
        if not attribute in self.get_features():
            raise ValueError("Attribute '{}' is not available".format(attribute))
        return self.query_search(None, [attribute], False)

    def callback_set_attributes_locked(self, arg):
        self.set_attributes_locked(arg['hashvalue'], arg['attributes'])

    def set_attributes_locked(self, hash, attributes):
        self.mutex.acquire()
        try:
            # create new connection from old one due to limitations of multi-threaded use (cursor initialization issue)
            with Database(self.database) as database:
                for attr in attributes:
                    cmd, name, value = attr[0], attr[1], attr[2]
                    database.submit('{} INTO {} (hash, value) VALUES ("{}", "{}")'.format(cmd, name, hash, value))
        finally:
            self.mutex.release()

    # Associate hashes with a hash-value in a group
    def set_attribute(self, attribute, value, hash_list, force):
        if not attribute in self.get_features():
            raise ValueError("Attribute '{}' is not available".format(attribute))
        with Database(self.database) as database:
            print("Setting {} to {} for benchmarks {}".format(attribute, value, hash_list))
            for h in hash_list:
                benchmark_administration.add_tag(database, attribute, value, h, force)

    # Remove association of a hash with a hash-value in a group
    def remove_attribute(self, attribute, value, hash_list):
        if not attribute in self.get_features():
            raise ValueError("Attribute '{}' is not available".format(attribute))
        with Database(self.database) as database:
            for h in hash_list:
                benchmark_administration.remove_tag(database, attribute, value, h)

    def search(self, attribute, hashvalue):
        if not attribute in self.get_features():
            raise ValueError("Attribute '{}' is not available".format(attribute))
        with Database(self.database) as database:
            return database.value_query("SELECT value FROM {} WHERE hash = '{}'".format(attribute, hashvalue))

    def hash_search(self, hashes=[], resolve=[], collapse=False, group_by=None):
        with Database(self.database) as database:
            try:
                return search.find_hashes(database, None, resolve, collapse, group_by, hashes, self.inner_separator, self.join_type)
            except sqlite3.OperationalError as err:
                raise ValueError("Query error for database '{}': {}".format(self.database, err))

    def query_search(self, query=None, resolve=[], collapse=False, group_by=None):
        with Database(self.database) as database:
            try:
                return search.find_hashes(database, query, resolve, collapse, group_by, [], self.inner_separator, self.join_type)
            except sqlite3.OperationalError as err:
                raise ValueError("Query error for database '{}': {}".format(self.database, err))
