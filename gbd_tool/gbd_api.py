# Global Benchmark Database (GBD)
# Copyright (C) 2020 Markus Iser, Luca Springer, Karlsruhe Institute of Technology (KIT)
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

from contextlib import ExitStack
from urllib.error import URLError

# internal packages
from gbd_tool import groups, benchmark_administration, search, bootstrap, sanitize
from gbd_tool.db import Database
from gbd_tool.gbd_hash import gbd_hash
from gbd_tool.util import eprint, is_number


class GbdApi:
    # Create a new GbdApi object which operates on the given databases
    def __init__(self, db_string, jobs=1, separator=" ", inner_separator=",", join_type="INNER"):
        self.databases = db_string.split(":")
        self.jobs = jobs
        self.mutex = multiprocessing.Lock()
        self.separator = separator
        self.inner_separator = inner_separator
        self.join_type = join_type

    def __enter__(self):
        self.database = Database(self.databases)
        with ExitStack() as stack:
            stack.enter_context(self.database)
            self._stack = stack.pop_all()
        return self

    def __exit__(self, exc_type, exc, traceback):
        self._stack.__exit__(exc_type, exc, traceback)

    # Calculate GBD hash
    @staticmethod
    def hash_file(path):
        return gbd_hash(path)

    # Import data from CSV file
    def import_file(self, path, key, source, target):
        benchmark_administration.import_csv(self.database, path, key, source, target, self.separator)

    # Initialize table 'local' with instances found under given path
    def init_database(self, path=None):
        eprint('Initializing local path entries {} using {} cores'.format(path, self.jobs))
        if self.jobs == 1 and multiprocessing.cpu_count() > 1:
            eprint("Activate parallel initialization using --jobs={}".format(multiprocessing.cpu_count()))
        benchmark_administration.remove_benchmarks(self.database)
        benchmark_administration.register_benchmarks(self, self.database, path, self.jobs)

    def bootstrap(self, named_algo):
        bootstrap.bootstrap(self, self.database, named_algo, self.jobs)

    def sanitize(self, hashes):
        sanitize.sanitize(self, self.database, hashes, self.jobs)

    def get_databases(self):
        return self.databases

    # Get all features (or those of given db)
    def get_features(self, path=None):
        if path == None:
            return self.database.tables_and_views()
        elif path in self.databases:
            with Database([path]) as db:
                return db.tables_and_views()
        else:
            return []

    # Get all material features (or those of given db)
    def get_material_features(self, path=None):
        if path == None:
            return self.database.tables()
        elif path in self.databases:
            with Database([path]) as db:
                return db.tables()
        else:
            return []

    # Get all virtual features (or those of given db)
    def get_virtual_features(self, path=None):
        if path == None:
            return self.database.views()
        elif path in self.databases:
            with Database([path]) as db:
                return db.views()
        else:
            return []

    # Check for existence of given feature
    def feature_exists(self, name):
        return name in self.database.tables()

    # Creates the given feature
    def create_feature(self, name, default_value):
        groups.add(self.database, name, default_value is not None, default_value)

    # Removes the given feature
    def remove_feature(self, name):
        groups.remove(self.database, name)

    # Retrieve information about a specific feature
    def get_feature_info(self, name):
        if not name in self.get_features():
            raise ValueError("Attribute '{}' is not available".format(name))
        values = self.database.table_values(name)
        return {'name': name, 
                'unique': self.database.table_unique(name),
                'default-value': self.database.table_default_value(name),
                'num-entries': self.database.table_size(name),
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
            # create new connection due to limitations of multi-threaded use (cursor initialization issue)
            with Database(self.databases) as db:
                for attr in attributes:
                    cmd, name, value = attr[0], attr[1], attr[2]
                    db.submit('{} INTO {} (hash, value) VALUES ("{}", "{}")'.format(cmd, name, hash, value))
        finally:
            self.mutex.release()

    # Set the attribute value for the given hashes
    def set_attribute(self, feature, value, hash_list, force):
        if not feature in self.get_material_features():
            raise ValueError("Attribute '{}' is not available (or virtual)".format(feature))
        for h in hash_list:
            benchmark_administration.add_tag(self.database, feature, value, h, force)

    # Remove the attribute value for the given hashes
    def remove_attributes(self, feature, hash_list):
        if not feature in self.get_material_features():
            raise ValueError("Attribute '{}' is not available (or virtual)".format(feature))
        benchmark_administration.remove_tags(self.database, feature, hash_list)

    def search(self, feature, hashvalue):
        if not feature in self.get_features():
            raise ValueError("Attribute '{}' is not available".format(feature))
        return self.database.value_query("SELECT value FROM {} WHERE hash = '{}'".format(feature, hashvalue))

    def hash_search(self, hashes=[], resolve=[], collapse=False, group_by=None):
        try:
            return search.find_hashes(self.database, None, resolve, collapse, group_by, hashes, self.inner_separator, self.join_type)
        except sqlite3.OperationalError as err:
            raise ValueError("Query error for database '{}': {}".format(self.databases, err))

    def query_search(self, query=None, resolve=[], collapse=False, group_by=None):
        try:
            return search.find_hashes(self.database, query, resolve, collapse, group_by, [], self.inner_separator, self.join_type)
        except sqlite3.OperationalError as err:
            raise ValueError("Query error for database '{}': {}".format(self.databases, err))

    def meta_set(self, feature, meta_feature, value):
        self.database.meta_set(feature, meta_feature, value)

    def meta_get(self, feature):
        return self.database.meta_get(feature)

    # clears sepcified meta-features of feature, 
    # or clears all meta-features if meta_feature is not specified
    def meta_clear(self, feature, meta_feature=None):
        self.database.meta_clear(feature, meta_feature)

    def calculate_par2_score(self, query, feature):
        info = self.meta_get(feature)
        if not "timeout" in info:
            eprint("Time-limit 'timeout' missing in meta-record of table '{}'.".format(feature))
            eprint("Unable to calculate score.")
            return
        if not "memout" in info:
            eprint("Memory-limit 'memout' missing in meta-record of table '{}'.".format(feature))
        if not "machine" in info:
            eprint("Machine-id 'machine' missing in meta-record of table '{}'.".format(feature))
        timeout = int(info["timeout"])
        times = self.query_search(query, [feature])
        score = 0
        penalized = set()
        for time in times:
            if is_number(time[1]):
                score += int(time[1])
            else:
                score += 2 * timeout
                penalized.add(time[1])
        print(score/len(times))
        print(penalized)


