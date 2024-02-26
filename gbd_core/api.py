
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
import pandas as pd

from contextlib import ExitStack
import traceback

from gbd_core.query import GBDQuery
from gbd_core.database import Database
from gbd_core.database import Schema
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


    @classmethod
    def identify(cls, path):
        """ Identify the given benchmark by its GBD hash 

            Args:
            path (str): path to benchmark

            Returns:
            str: GBD hash
        """
        from gbd_core.contexts import identify
        return identify(path)


    def query(self, gbd_query=None, hashes=[], resolve=[], collapse="group_concat", group_by=None, join_type="LEFT"):
        """ Query the database

            Args:
            gbd_query (str): GBD query string
            hashes (list): list of hashes (=benchmark ids), the query is restricted to
            resolve (list): list of features to be resolved
            collapse (str): collapse function: min, max, avg, count, sum, group_concat, or none
            group_by (str): group results by that feature instead of hash (default)
            join_type (str): join type: left or inner

            Returns:
            pandas.DataFrame: query result
        """
        query_builder = GBDQuery(self.database, gbd_query)
        try:
            sql = query_builder.build_query(hashes, resolve, group_by, join_type, collapse)
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
        group = group_by or query_builder.determine_group_by(resolve)
        cols = [ p.split(':') for p in [ group ] + resolve ]
        cols = [ c[0] if len(c) == 1 else c[1] for c in cols ]
        return pd.DataFrame(result, columns=cols)


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
        if not len(hashes):
            raise GBDException("No hashes given")
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
        if len(values) and len(hashes):
            for values_slice in util.slice_iterator(values, 10):
                for hashes_slice in util.slice_iterator(hashes, 10):
                    self.database.delete(feature, values_slice, hashes_slice, target_db)
        elif len(values):
            for values_slice in util.slice_iterator(values, 10):
                self.database.delete(feature, values_slice, [], target_db)
        elif len(hashes):
            for hashes_slice in util.slice_iterator(hashes, 10):
                self.database.delete(feature, [], hashes_slice, target_db)


    def delete_hashes(self, hashes, target_db=None):
        """ Delete all values for given hashes
            
            Args:
            hashes (list): list of hashes (=benchmark ids) to be deleted
            target_db (str, optional): name of target database
            if None, default database (first in list) is used

            Raises:
            GBDException, if feature does not exist
        """
        if not len(hashes):
            raise GBDException("No hashes given")
        self.database.delete_hashes_entirely(hashes, target_db)


    def get_databases(self, context=None):
        """ Get list of database names

            Returns: list of database names
        """
        if context is None:
            return list(self.database.get_databases())
        else:
            return [ db for db in self.database.get_databases() if self.database.dcontext(db) == context ]


    def get_database_path(self, dbname):
        """ Get path for given database name

            Args:
            dbname (str): name of database

            Returns: path to database
        """
        return self.database.dpath(dbname)
    

    @classmethod
    def get_database_name(self, path):
        """ Get database name for given path

            Args:
            path (str): path to database

            Returns: name of database
        """
        return Schema.dbname_from_path(path)
    

    def get_contexts(self, dbs=[]):
        """ Get list of contexts

            Returns: list of contexts
        """
        if not len(dbs):
            return list(self.database.get_contexts())
        else:
            return list(set([ self.database.dcontext(db) for db in dbs ]))


    def get_feature_info(self, fname):
        """ Retrieve information about a specific feature"""
        finfo = self.database.find(fname)
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
        lst = self.database.get_features([] if not dbname else [dbname])
        if "hash" in lst:
            lst.remove("hash")
        return lst

    
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


    def copy_feature(self, old_name, new_name, target_db=None, gbd_query=None, hashes=[]):
        """ Copies feature with given name

            Args:
            old_name (str): old feature name
            new_name (str): new feature name
            target_db (str): name of database to copy feature to
            if None, default database (fist in list) is used

            Returns: None
        """
        if not self.feature_exists(old_name):
            raise GBDException("Feature '{}' does not exist".format(old_name))
        
        if not self.feature_exists(new_name, target_db):
            self.create_feature(new_name, target_db=target_db)

        hashes = list(self.query(gbd_query=gbd_query, hashes=hashes)["hash"])

        self.database.copy_feature(old_name, new_name, target_db, hashes)

