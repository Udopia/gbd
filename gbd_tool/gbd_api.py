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
import tatsu
import csv
import platform

from contextlib import ExitStack

# internal packages
from gbd_tool import benchmark_administration, search, bootstrap
from gbd_tool.db import Database
from gbd_tool.util import eprint, is_number
from gbd_tool.error import *

try:
    from gbdhashc import gbdhash as gbd_hash
except ImportError:
    from gbd_tool.gbd_hash import gbd_hash


class GbdApi:

    # Create a new GbdApi object which operates on the given databases
    def __init__(self, db_string, jobs=1, separator=" ", join_type="LEFT", verbose=False):
        if platform.system() == "Windows":
            self.databases = db_string.split(";")
        else:
            self.databases = db_string.split(":")
        self.jobs = jobs
        self.mutex = multiprocessing.Lock()
        self.separator = separator
        self.join_type = join_type
        self.verbose = verbose

    def __enter__(self):
        self.database = Database(self.databases, self.verbose)
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
        if not self.feature_exists(target):
            print("Feature {} does not exist. Import canceled.".format(target))
        with open(path, newline='') as csvfile:
            csvreader = csv.DictReader(csvfile, delimiter=self.separator, quotechar='\'')
            lst = [(row[key].strip(), row[source].strip()) for row in csvreader if row[source] and row[source].strip()]
            print("Inserting {} values into group {}".format(len(lst), target))
            self.database.bulk_insert(target, lst)

    # Initialize table 'local' with instances found under given path
    def init_database(self, path=None):
        eprint('Initializing local path entries {} using {} cores'.format(path, self.jobs))
        if self.jobs == 1 and multiprocessing.cpu_count() > 1:
            eprint("Activate parallel initialization using --jobs={}".format(multiprocessing.cpu_count()))
        benchmark_administration.remove_benchmarks(self.database)
        benchmark_administration.register_benchmarks(self, self.database, path, self.jobs)

    def bootstrap(self, named_algo, hashes):
        bootstrap.bootstrap(self, self.database, named_algo, hashes, self.jobs)

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
    def create_feature(self, name, default_value=None):
        self.database.create_table(name, default_value)

    # Removes the given feature
    def remove_feature(self, name):
        self.database.delete_table(name)

    # Rename the given feature
    def rename_feature(self, old_name, new_name):
        self.database.rename_table(old_name, new_name)

    def get_feature_size(self, name):
        if not name in self.get_features():
            raise GbdApiFeatureNotFound("Feature '{}' not found".format(name))
        return self.database.table_size(name)

    # Retrieve information about a specific feature
    def get_feature_info(self, name, path=None):
        if path is None:
            system_record = self.database.system_record(name)
            meta_record = self.database.meta_record(name)
            return {**system_record, **meta_record}
        else:
            with Database([path]) as db:
                system_record = db.system_record(name)
                meta_record = db.meta_record(name)
                return {**system_record, **meta_record}

    # Retrieve system information about a specific feature
    def get_feature_system_record(self, name, path=None):
        if path is None:
            system_record = self.database.system_record(name)
            return {**system_record}
        else:
            with Database([path]) as db:
                system_record = db.system_record(name)
                return {**system_record}

    # Retrieve meta information about a specific feature
    def get_feature_meta_record(self, name, path=None):
        if path is None:
            meta_record = self.database.meta_record(name)
            return {**meta_record}
        else:
            with Database([path]) as db:
                meta_record = db.meta_record(name)
                return {**meta_record}

    def meta_set(self, feature, meta_feature, value):
        self.database.meta_set(feature, meta_feature, value)

    # clears sepcified meta-features of feature, 
    # or clears all meta-features if meta_feature is not specified
    def meta_clear(self, feature, meta_feature=None):
        self.database.meta_clear(feature, meta_feature)

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
            raise GbdApiFeatureNotFound("Feature '{}' not found".format(feature))
        values = ', '.join(['("{}", "{}")'.format(hash, value) for hash in hash_list])
        if self.database.table_unique(feature):
            if force:
                self.database.submit('DELETE FROM {} WHERE hash IN ("{}")'.format(feature, '", "'.join(hash_list)))
            try:
                self.database.submit('REPLACE INTO {} (hash, value) VALUES {}'.format(feature, values))
            except sqlite3.IntegrityError as err:
                # thrown if existing value is not the default value or equal to the value to be set
                # requires the unique on insert-triggers introduced in version 3.0.9
                eprint(str(err) + ": Use the force!")
        else:
            try:
                self.database.submit('INSERT INTO {} (hash, value) VALUES {}'.format(feature, values))
            except Exception as err:
                # thrown if hash+value combination is already set
                # requires the unique constraint introduced in version 3.0.9
                eprint(err)

    # Remove the attribute value for the given hashes
    def remove_attributes(self, feature, hash_list):
        if not feature in self.get_material_features():
            raise GbdApiFeatureNotFound("Feature '{}' not found".format(feature))
        self.database.submit("DELETE FROM {} WHERE hash IN ('{}')".format(feature, "', '".join(hash_list)))

    def set_tag(self, tag_feature, tag_value, hash_list):
        self.database.set_tag(tag_feature, tag_value, hash_list)

    def search(self, feature, hashvalue):
        if not feature in self.get_features():
            raise GbdApiFeatureNotFound("Feature '{}' not found".format(feature))
        return self.database.value_query("SELECT value FROM {} WHERE hash = '{}'".format(feature, hashvalue))

    def query_search(self, query=None, hashes=[], resolve=[], collapse="GROUP_CONCAT", group_by="hash"):
        try:
            sql = search.build_query(query, hashes, resolve or [], collapse, group_by or "hash", self.join_type)
            return self.database.query(sql)
        except sqlite3.OperationalError as err:
            raise GbdApiDatabaseError("Make sure the feature given in the query does exist")
        except tatsu.exceptions.FailedParse as err:
            raise GbdApiParsingFailed("Tatsu could not parse query: {}' - {}".format(query, err.message))

    def calculate_par2_score(self, query, name, timeout):
        times = self.query_search(query, [], [name])
        return sum(float(time[1]) if is_number(time[1]) and float(time[1]) < timeout else 2*timeout for time in times) / len(times)

    def calculate_vbs_par2(self, query, names, timeout):
        result = self.query_search(query, [], names)
        return sum([min(float(val) if is_number(val) else 2*timeout for val in row[1:]) for row in result]) / len(result)

    def calculate_vbs(self, query, names, timeout):
        result = self.query_search(query, [], names)
        return [(row[0], min(float(val) if is_number(val) else 2*timeout for val in row[1:])) for row in result]
