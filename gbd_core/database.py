# GBD Benchmark Database (GBD)
# Copyright (C) 2021 Markus Iser, Karlsruhe Institute of Technology (KIT)
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
import typing

from pprint import pprint

from gbd_core.util import eprint
from gbd_core.schema import Schema, FeatureInfo
from gbd_core import contexts


class DatabaseException(Exception):
    pass


class Database:

    def __init__(self, path_list, verbose=False, autocommit=True):
        self.verbose = verbose
        self.schemas = self.init_schemas(path_list)
        self.features = self.init_features()
        self.connection = sqlite3.connect("file::memory:?cache=shared", uri=True, timeout=10)
        self.cursor = self.connection.cursor()
        self.maindb = None
        self.autocommit = autocommit
        schema: Schema
        for schema in self.schemas.values():
            if not schema.is_in_memory():
                self.execute("ATTACH DATABASE '{}' AS {}".format(schema.path, schema.dbname))
            if not self.maindb:
                self.maindb = schema.dbname  # target of inserts and updates
        
    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.connection.commit()
        self.connection.close()


    # returns major version of sqlite3 as float
    @classmethod
    def sqlite3_version(cls):
        return float(sqlite3.sqlite_version.rsplit('.', 1)[0])


    def init_schemas(self, path_list) -> typing.Dict[str, Schema]:
        result = dict()
        for path in path_list:
            schema = Schema.create(path)
            if not schema.dbname in result:
                result[schema.dbname] = schema
            elif schema.is_in_memory():
                result[schema.dbname].absorb(schema)
            else:
                raise DatabaseException("Database name collision on " + schema.dbname)
        return result

    
    def is_features_table(self, info: FeatureInfo):
        return contexts.prepend_context("features", info.context) == info.table

    def init_features(self) -> typing.Dict[str, FeatureInfo]:
        result = dict()
        schema: Schema
        for schema in self.schemas.values():
            feature: FeatureInfo
            for feature in schema.features.values():
                # first found feature is used: (=feature precedence by database position)
                if not feature.name in result:
                    result[feature.name] = feature
                elif feature.column == "hash" and self.is_features_table(feature): 
                    # first found features table is the one that serves the hash
                    if not self.is_features_table(result[feature.name]):
                        result[feature.name] = feature
                #else:
                #    eprint("Warning: Feature name collision on {}. Using first occurence in {}.".format(feature.name, result[feature.name].database))
        return result


    def query(self, q):
        if self.verbose:
            eprint(q)
        return self.cursor.execute(q).fetchall()

    def execute(self, q):
        if self.verbose:
            eprint(q)
        self.cursor.execute(q)
        if self.autocommit:
            self.commit()

    def commit(self):
        self.connection.commit()

    def set_auto_commit(self, autocommit):
        self.autocommit = autocommit


    def dexists(self, dbname):
        return dbname in self.schemas.keys()

    def dmain(self, dbname):
        return dbname == self.maindb

    def dpath(self, dbname):
        return self.schemas[dbname].path
        
    def dcontexts(self, dbname):
        return self.schemas[dbname].get_contexts()
        
    def dtables(self, dbname):
        return self.schemas[dbname].get_tables()
        
    def dviews(self, dbname):
        return self.schemas[dbname].get_views()


    def fexists(self, feature):
        return feature in self.features.keys()

    def fexists_or_raise(self, feature):
        if not self.fexists(feature):
            raise DatabaseException("Feature {} does not exists".format(feature))

    def fdatabase(self, feature):
        return self.features[feature].database

    def fcontext(self, feature):
        return self.features[feature].context

    def ftable(self, feature, full=True):
        return self.features[feature].table if not full else "{}.{}".format(self.fdatabase(feature), self.features[feature].table)

    def fcolumn(self, feature):
        return self.features[feature].column

    def fdefault(self, feature):
        return self.features[feature].default

    def fvirtual(self, feature):
        return self.features[feature].virtual


    def get_databases(self):
        return self.schemas.keys()
  
    def get_features(self, tables=True, views=True, db=None):
        return [ f for (f, i) in self.features.items() if ((views and i.virtual) or (tables and not i.virtual)) and (not db or db == i.database) ]

    def get_tables(self, context = None, db = None):
        tables = [ i.table for i in self.features.values() if not i.virtual and (not context or context == i.context) and (not db or db == i.database) ]
        return list(set(tables))


    def create_feature(self, name, default_value=None, target_db=None, permissive=False):
        db = target_db or self.maindb
        created = self.schemas[db].create_feature(name, default_value, permissive)
        for finfo in created:
            # this code disregards feature precedence by database position:
            if not finfo.name in self.features.keys():
                self.features[finfo.name] = finfo


    def set_values(self, feature, value, hashes, target_db=None):
        # select target_db or database by feature precedence:
        database = target_db or self.features[feature].database
        self.schemas[database].set_values(feature, value, hashes)


    def rename_feature(self, old_name, new_name):
        # select database by feature precedence:
        Schema.valid_feature_or_raise(new_name)
        self.fexists_or_raise(old_name)
        table = self.features[old_name].table
        self.execute("ALTER TABLE {} RENAME COLUMN {} TO {}".format(table, old_name, new_name))
        if not self.features[old_name].default:
            self.execute("ALTER TABLE {} RENAME TO {}".format(old_name, new_name))
        self.features[new_name] = self.features.pop(old_name)
        self.features[new_name].name = new_name

    def delete_feature(self, name):
        # select database by feature precedence:
        self.fexists_or_raise(name)
        if not self.features[name].default:
            self.execute('DROP TABLE IF EXISTS {}'.format(name))
            self.execute('DROP TRIGGER IF EXISTS {}_dval'.format(name))
            context = contexts.context_from_name(name)
            if name == contexts.prepend_context("local", context):
                filename = contexts.prepend_context("filename", context)
                self.execute('DROP VIEW IF EXISTS {}'.format(filename))
        elif Database.sqlite3_version() >= 3.35:
            table = self.features[name].table
            self.execute("ALTER TABLE {} DROP COLUMN {}".format(table, name))
        else:
            raise DatabaseException("Cannot delete unique feature {} in SQLite version < 3.35".format(name))
        self.features.pop(name)

    def delete(self, feature, values=[], hashes=[]):
        # select database by feature precedence:
        self.fexists_or_raise(feature)
        database = self.features[feature].database
        table = self.features[feature].table
        column = self.features[feature].column
        default = self.features[feature].default
        w1 = "{col} IN ('{v}')".format(col=column, v="', '".join(values)) if len(values) else "1=1"
        w2 = "hash IN ('{h}')".format(h="', '".join(hashes)) if len(hashes) else "1=1"
        where = "{} AND {}".format(w1, w2)
        if not default:
            hashlist = [ r[0] for r in self.query("SELECT DISTINCT(hash) FROM {d}.{tab} WHERE {w}".format(d=database, tab=feature, w=where)) ]
            self.execute("DELETE FROM {d}.{tab} WHERE {w}".format(d=database, tab=feature, w=where))
            remaining = [ r[0] for r in self.query("SELECT DISTINCT(hash) FROM {d}.{tab} WHERE hash in ('{h}')".format(d=database, tab=feature, h="', '".join(hashlist))) ]
            setnone = [ h for h in hashlist if not h in remaining ]
            self.execute("UPDATE {d}.{tab} SET {col} = 'None' WHERE hash IN ('{h}')".format(d=database, tab="features", col=feature, h="', '".join(setnone)))
        else:
            self.execute("UPDATE {d}.{tab} SET {col} = '{default}' WHERE {w}".format(d=database, tab=table, col=column, default=default, w=where))


    def feature_info(self, feature):
        self.fexists_or_raise(feature)
        result = dict()
        table = self.features[feature].table
        column = self.features[feature].column
        default = self.features[feature].default
        result['feature_name'] = feature
        result['feature_count'] = self.query('SELECT COUNT(*) FROM {}'.format(table))[0][0]
        result['feature_default'] = default
        minmax = self.query('SELECT MIN(CAST({col} AS NUMERIC)), MAX(CAST({col} AS NUMERIC)) FROM {tab} WHERE NOT {col} GLOB "*[^0-9.e\-]*" AND {col} LIKE "_%"'.format(col=column, tab=table))
        if len(minmax):
            result['feature_min'] = minmax[0][0]
            result['feature_max'] = minmax[0][1]
        values = self.query('SELECT DISTINCT {col} FROM {tab} WHERE {col} GLOB "*[^0-9.e\-]*" OR {col} NOT LIKE "_%"'.format(col=column, tab=table))
        result['feature_values'] = " ".join([x[0] for x in values])
        return result