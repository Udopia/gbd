# GBD Benchmark Database (GBD)
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
import os

from contextlib import ExitStack

# internal packages
from gbd_tool import query_builder
from gbd_tool.db import Database
from gbd_tool.util import eprint

from gbd_tool.gbd_hash import gbd_hash

try:
    from gbdc import extract_base_features as extract
except ImportError:
    def extract(path):
        raise GBDException("Method 'extract' not available")


class GBDException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class GBD:
    # Create a new GBD object which operates on the given databases
    def __init__(self, db_string, jobs=1, separator=" ", join_type="LEFT", verbose=False):
        self.databases = db_string.split(os.pathsep)
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

    @staticmethod
    def extract_base_features(path):
        print(extract(path))

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
        if not self.feature_exists(name):
            self.database.create_table(name, default_value)
        else:
            raise GBDException("Feature '{}' does already exist".format(name))

    # Removes the given feature
    def remove_feature(self, name):
        if not self.feature_exists(name):
            self.database.delete_table(name)
        else:
            raise GBDException("Feature '{}' does not exist or is virtual".format(name))

    # Rename the given feature
    def rename_feature(self, old_name, new_name):
        if not self.feature_exists(old_name):
            raise GBDException("Feature '{}' does not exist or is virtual".format(old_name))
        elif self.feature_exists(new_name):
            raise GBDException("Feature '{}' does already exist".format(new_name))
        else:
            self.database.rename_table(old_name, new_name)

    def get_feature_size(self, name):
        if not name in self.get_features():
            raise GBDException("Feature '{}' not found".format(name))
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
    def set_attribute(self, feature, value, query, hashes=[], force=False):
        if not feature in self.get_material_features():
            raise GBDException("Feature '{}' missing or virtual".format(feature))
        hash_list = [hash[0] for hash in self.query_search(query, hashes)]
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
            raise GBDException("Feature '{}' not found".format(feature))
        self.database.submit("DELETE FROM {} WHERE hash IN ('{}')".format(feature, "', '".join(hash_list)))

    def search(self, feature, hashvalue):
        if not feature in self.get_features():
            raise GBDException("Feature '{}' not found".format(feature))
        return self.database.value_query("SELECT value FROM {} WHERE hash = '{}'".format(feature, hashvalue))

    def query_search(self, query=None, hashes=[], resolve=[], collapse="GROUP_CONCAT", group_by="hash"):
        try:
            sql = query_builder.build_query(query, hashes, resolve or [], collapse, group_by or "hash", self.join_type)
            return self.database.query(sql)
        except sqlite3.OperationalError as err:
            raise GBDException("Database Operational Error: {}".format(str(err)))
        except tatsu.exceptions.FailedParse as err:
            raise GBDException("Parser Error: {}".format(str(err)))
