
# MIT License

# Copyright (c) 2023 Markus Iser, Karlsruhe Institute of Technology (KIT)

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.


import sqlite3
import tatsu
import os
import pandas as pd

from contextlib import ExitStack
import traceback

from gbd_core.query import GBDQuery
from gbd_core.database import Database
from gbd_core import util


class GBDException(Exception):
    pass


class GBD:
    # Create a new GBD object which operates on the given databases
    def __init__(self, dbs: list, verbose: bool=False):
        assert(isinstance(dbs, list))
        self.database = Database(dbs, verbose)
        self.verbose = verbose

    def __enter__(self):
        with ExitStack() as stack:
            stack.enter_context(self.database)
            self._stack = stack.pop_all()
        return self

    def __exit__(self, exc_type, exc, traceback):
        self._stack.__exit__(exc_type, exc, traceback)


    def query(self, gbd_query=None, hashes=[], resolve=[], collapse="group_concat", group_by="hash", join_type="LEFT", subselect=False):
        query_builder = GBDQuery(self.database, gbd_query)
        try:
            sql = query_builder.build_query(hashes, resolve or [], group_by or "hash", join_type, collapse, subselect)
        except tatsu.exceptions.FailedParse as err:
            if self.verbose:
                util.eprint(traceback.format_exc())
            raise GBDException("Parser Error with Query '{}': {}".format(gbd_query, str(err)))
        try:
            result = self.database.query(sql)
        except sqlite3.OperationalError as err:
            if self.verbose:
                util.eprint(traceback.format_exc())
            raise GBDException("Database Operational Error: {}".format(str(err)))
        return pd.DataFrame(result, columns=[ group_by ] + (resolve or []))


    def set_values(self, name, value, hashes, target_db=None):
        """ Set feature value for given hashes 
            
            Args:
                name (str): feature name
                value (str): value to be set
                hashes (list): list of hashes (=benchmark ids)
                target_db (str, optional): name of target database
                    if None, default database (first in list) is used

            Raises:
                GBDException, if feature does not exist
        """
        if not self.feature_exists(name, target_db):
            raise GBDException("Feature '{}' does not exist".format(name))
        self.database.set_values(name, value, hashes, target_db)


    def reset_values(self, feature, values=[], hashes=[], target_db=None):
        """ Reset feature value for given hashes 
            
            Args:
                feature (str): feature name
                values (list, optional): list of values to be reset
                hashes (list, optional): list of hashes (=benchmark ids) to be reset
                target_db (str, optional): name of target database
                    if None, default database (first in list) is used

            Raises:
                GBDException, if feature does not exist
        """
        if not self.feature_exists(feature, target_db):
            raise GBDException("Feature '{}' does not exist".format(feature))
        for values_slice in util.slice_iterator(values, 10):
            for hashes_slice in util.slice_iterator(hashes, 10):
                self.database.delete(feature, values_slice, hashes_slice, target_db)


    def get_databases(self):
        """ Get list of database names

            Returns: list of database names
        """
        return list(self.database.get_databases())


    def get_database_path(self, dbname):
        """ Get path for given database name

            Args:
                dbname (str): name of database

            Returns: path to database
        """
        return self.database.dpath(dbname)


    # Retrieve information about a specific feature
    def get_feature_info(self, fname):
        finfo = self.database.finfo(fname)
        df = self.query(resolve=[ fname ], collapse=None)
        numcol = df[fname].apply(lambda x: pd.to_numeric(x, errors = 'coerce'))
        return {
            'feature_name': fname,
            'feature_count': len(df.index),
            'feature_default': finfo.default,
            'feature_min': numcol.min(),
            'feature_max': numcol.max(),
            'feature_values': " ".join([ val for val in df[fname].unique() if val and not util.is_number(val) ])
        }

    
    def get_features(self, dbname: str=None):
        """ Get features from the database.

            Args:
                dbname (str): name of feature database
                    if None, feature list is accumulated over all databases

            Returns: list of features names
        """
        return self.database.get_features([] if not dbname else [dbname])

    
    def feature_exists(self, name, dbname=None):
        """ Check if feature exists in the database.

            Args:
                name (str): name of feature
                dbname (str): name of feature database
                    if None, feature existence is checked for in all databases

            Returns: True if feature exists in dbname or any database, False otherwise
        """
        return name in self.get_features(dbname)


    def create_feature(self, name: str, default_value: str=None, target_db: str=None):
        """ Creates feature with given name

            Args:
                name (str): feature name
                default_value (str): default value for 1:1 features
                    if None, a multi-valued (1:n) feature is created
                target_db (str): database name 
                    if None, default database (fist in list) is used

            Returns: None

            Raises: 
                GBDException, if feature already exists in target_db
        """
        if not self.feature_exists(name, target_db):
            self.database.create_feature(name, default_value, target_db, False)
        else:
            raise GBDException("Feature '{}' does already exist".format(name))

    
    def delete_feature(self, name, target_db=None):
        """ Deletes feature with given name

            Args:
                name (str): feature name
                target_db (str): database name 
                    if None, default database (fist in list) is used

            Returns: None

            Raises: 
                GBDException, if feature does not exist in target_db
        """
        if self.feature_exists(name, target_db):
            self.database.delete_feature(name, target_db)
        else:
            raise GBDException("Feature '{}' does not exist".format(name))

    
    def rename_feature(self, old_name, new_name, target_db=None):
        """ Renames feature with given name

            Args:
                old_name (str): old feature name
                new_name (str): new feature name
                target_db (str): database name 
                    if None, default database (fist in list) is used

            Returns: None

            Raises: 
                GBDException, 
                    - if feature 'old_name' does not exist in target_db
                    - if feature 'new_name' already exists in target_db
        """
        if not self.feature_exists(old_name, target_db):
            raise GBDException("Feature '{}' does not exist".format(old_name))
        elif self.feature_exists(new_name, target_db):
            raise GBDException("Feature '{}' does already exist".format(new_name))
        else:
            self.database.rename_feature(old_name, new_name, target_db)
