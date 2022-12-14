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


import sqlite3
import tatsu
import os
import pandas as pd

from contextlib import ExitStack

from gbd.query_builder import GBDQuery
from gbd.database import Database
from gbd.util import eprint


class GBDException(Exception):
    pass


class GBD:
    # Create a new GBD object which operates on the given databases
    def __init__(self, dbs, context='cnf', jobs=1, tlim=5000, mlim=2000, flim=1000, join_type="LEFT", verbose=False):
        self.databases = dbs if isinstance(dbs, list) else dbs.split(os.pathsep)
        self.context = context
        self.jobs = jobs
        self.tlim = tlim  # time limit (seconds)
        self.mlim = mlim  # memory limit (mega bytes)
        self.flim = flim  # file size limit (mega bytes)
        self.join_type = join_type
        self.verbose = verbose
        self.database = Database(self.databases, self.verbose)

    def __enter__(self):
        with ExitStack() as stack:
            stack.enter_context(self.database)
            self._stack = stack.pop_all()
        return self

    def __exit__(self, exc_type, exc, traceback):
        self._stack.__exit__(exc_type, exc, traceback)

    def query(self, gbd_query=None, hashes=[], resolve=[], collapse="group_concat", group_by="hash", subselect=False):
        query_builder = GBDQuery(self.database, self.join_type, collapse, subselect)
        try:
            sql = query_builder.build_query(gbd_query, hashes, resolve or [], group_by or "hash")
        except tatsu.exceptions.FailedParse as err:
            raise GBDException("Parser Error: {}".format(str(err)))
        try:
            result = self.database.query(sql)
        except sqlite3.OperationalError as err:
            raise GBDException("Database Operational Error: {}".format(str(err)))
        return pd.DataFrame(result, columns=[ group_by ] + (resolve or []))

    # Get all features
    def get_features(self, tables=True, views=True, dbname=None):
        return self.database.get_features(tables=tables, views=views, db=dbname)

    # Check for existence of given feature
    def feature_exists(self, name):
        return name in self.get_features()

    # Retrieve information about a specific feature
    def get_feature_info(self, name):
        return self.database.feature_info(name)

    def get_limits(self) -> dict():
        return { 'tlim': self.tlim, 'mlim': self.mlim, 'flim': self.flim }

    def get_databases(self):
        return list(self.database.get_databases())

    def get_database_path(self, dbname):
        return self.database.dpath(dbname)

    # Creates feature of given name
    def create_feature(self, name, default_value=None):
        if not self.feature_exists(name):
            self.database.create_feature(name, default_value)
        else:
            raise GBDException("Feature '{}' does already exist".format(name))

    # Removes feature of given name
    def delete_feature(self, name):
        if self.feature_exists(name):
            self.database.delete_feature(name)
        else:
            raise GBDException("Feature '{}' does not exist or is virtual".format(name))

    # Rename the given feature
    def rename_feature(self, old_name, new_name):
        if not self.feature_exists(old_name):
            raise GBDException("Feature '{}' does not exist or is virtual".format(old_name))
        elif self.feature_exists(new_name):
            raise GBDException("Feature '{}' does already exist".format(new_name))
        else:
            self.database.rename_feature(old_name, new_name)

    # Set the attribute value for the given hashes
    def set_attribute(self, feature, value, query, hashes=[], force=False):
        if not feature in self.get_features(views=False):
            raise GBDException("Feature '{}' missing or virtual".format(feature))
        hash_list = hashes if not query else self.query(query, hashes)['hash'].tolist()
        try:
            self.database.set_values(feature, value, hash_list)
        except Exception as err:
            raise GBDException(str(err))

    # Remove the attribute value for the given hashes
    def remove_attributes(self, feature, values=[], hashes=[]):
        if not feature in self.get_features(views=False):
            raise GBDException("Feature '{}' not found or virtual".format(feature))
        self.database.delete(feature, values, hashes)