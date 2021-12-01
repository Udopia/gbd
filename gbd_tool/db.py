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

from gbd_tool.gbd_hash import HASH_VERSION
from gbd_tool.util import eprint, prepend_context, context_from_name, make_alnum_ul
from gbd_tool.schema import Schema, FeatureInfo


class DatabaseException(Exception):
    pass


class Database:
    INMEMORYDB = "file::memory:?cache=shared"

    def __init__(self, path_list, verbose=False):
        self.verbose = verbose
        self.schemas = self.init_schemas(path_list)
        self.features = self.init_features()
        self.connection = sqlite3.connect(Schema.IN_MEMORY_DB, uri=True, timeout=10)
        self.cursor = self.connection.cursor()
        self.maindb = None
        schema: Schema
        for schema in self.schemas.values():
            if not schema.csv:
                self.cursor.execute("ATTACH DATABASE '{}' AS {}".format(schema.path, schema.dbname))
            if not self.maindb:
                self.maindb = schema.dbname  # target of inserts and updates
        
    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.connection.commit()
        self.connection.close()


    def init_schemas(self, path_list) -> typing.Dict[str, Schema]:
        result = dict()
        for path in path_list:
            schema: Schema = Schema(path)
            if not schema.dbname in result:
                result[schema.dbname] = schema
            elif schema.csv:
                result[schema.dbname].absorb(schema)
            else:
                raise DatabaseException("Database name collision on " + schema.dbname)
        return result

    def init_features(self) -> typing.Dict[str, FeatureInfo]:
        result = dict()
        schema: Schema
        for schema in self.schemas.values():
            feature: FeatureInfo
            for feature in schema.features:
                if not feature.name in result:
                    result[feature.name] = feature
                elif feature.column == "hash": 
                    if not Schema.is_main_hash_column(result[feature.name]) and Schema.is_main_hash_column(feature):
                        result[feature.name] = feature
                else:
                    eprint("Warning: Feature name collision on {}. Using first occurence in {}.".format(feature.name, feature.database))
        return result


    def query(self, q):
        if self.verbose:
            eprint(q)
        return self.cursor.execute(q).fetchall()

    def execute(self, q):
        if self.verbose:
            eprint(q)
        self.cursor.execute(q)
        self.connection.commit()


    def dexists(self, dbname):
        return dbname in self.schemas.keys()

    def dmain(self, dbname):
        return dbname == self.maindb

    def dpath(self, dbname):
        return self.schemas[dbname].path
        
    def dcontexts(self, dbname):
        return self.schemas[dbname].contexts
        
    def dtables(self, dbname):
        return self.schemas[dbname].tables
        
    def dviews(self, dbname):
        return self.schemas[dbname].views


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
  
    def get_features(self, tables=True, views=True, database=None):
        result = []
        for (feature, info) in self.features.items():
            if not views and info.virtual:
                continue
            if not tables and not info.virtual:
                continue
            if database and database != info.database:
                continue
            result.append(feature)
        return result

    def get_tables(self, context = None, dbname = None):
        tables = list()
        for finfo in self.features.values():
            if (not context or context == finfo.context) and (not dbname or dbname == finfo.database) and not finfo.virtual:
                if not finfo.table in tables:
                    tables.append(finfo.table)
        return tables


    def create_feature(self, name, default_value=None, permissive=False):
        if not permissive:  # internal use can be unchecked, e.g., to create the reserved feature local
            Schema.valid_feature_or_raise(name)
        if self.fexists(name):
            if not permissive:
                raise DatabaseException("Feature {} exists".format(name))
            else:
                return
        context = context_from_name(name)
        if default_value is not None:
            features = prepend_context("features", context)
            self.schemas[self.maindb].create_main_feature_table(context_from_name(name))
            self.execute('ALTER TABLE {}.{} ADD {} TEXT NOT NULL DEFAULT {}'.format(self.maindb, features, name, default_value))
            # update schema
            self.features[name] = FeatureInfo(name, self.maindb, context, features, name, default_value, False)
            self.schemas[self.maindb].features.append(self.features[name])
            # initialize hash
            hashfeature = prepend_context("hash", context)
            if not hashfeature in self.features:
                self.features[hashfeature] = FeatureInfo(hashfeature, self.maindb, context, features, "hash", None, False)
                self.schemas[self.maindb].features.append(self.features[hashfeature])
        else:
            self.execute('CREATE TABLE IF NOT EXISTS {}.{} (hash TEXT NOT NULL, value TEXT NOT NULL, CONSTRAINT all_unique UNIQUE(hash, value))'.format(self.maindb, name))
            # insert default values for new hashes into features table
            features = prepend_context("features", context)
            self.execute("CREATE TRIGGER IF NOT EXISTS {}.{}_dval AFTER INSERT ON {} BEGIN INSERT OR IGNORE INTO {} (hash) VALUES (NEW.hash); END".format(self.maindb, name, name, features))
            # create "filename" view for "local" tables
            if name == prepend_context("local", context):
                filename = prepend_context("filename", context)
                self.execute("CREATE VIEW IF NOT EXISTS {}.{} (hash, value) AS SELECT hash, REPLACE(value, RTRIM(value, REPLACE(value, '/', '')), '') FROM {}".format(self.maindb, filename, name))
            # update contexts
            if not context in self.schemas[self.maindb].contexts:
                self.schemas[self.maindb].contexts.append(context)
            # update schema
            self.features[name] = FeatureInfo(name, self.maindb, context, name, "value", None, False)
            self.schemas[self.maindb].features.append(self.features[name])
            self.schemas[self.maindb].tables.append(name)
            # initialize hash
            hashfeature = prepend_context("hash", context)
            if not hashfeature in self.features:
                self.features[hashfeature] = FeatureInfo(hashfeature, self.maindb, context, name, "hash", None, False)
                self.schemas[self.maindb].features.append(self.features[hashfeature])


    def rename_feature(self, old_name, new_name):
        Schema.valid_feature_or_raise(new_name)
        self.fexists_or_raise(old_name)
        if not self.features[old_name].default:
            self.execute("ALTER TABLE {} RENAME TO {}".format(old_name, new_name))
        else:
            table = self.features[old_name].table
            self.execute("ALTER TABLE {} RENAME COLUMN {} TO {}".format(table, old_name, new_name))
        self.features[new_name] = self.features.pop(old_name)
        self.features[new_name].name = new_name

    def delete_feature(self, name):
        self.fexists_or_raise(name)
        if not self.features[name].default:
            self.execute('DROP TABLE IF EXISTS {}'.format(name))
            self.execute('DROP TRIGGER IF EXISTS {}_dval'.format(name))
        else:
            table = self.features[name].table
            self.execute("ALTER TABLE {} DROP COLUMN {}".format(table, name))
        self.features.pop(name)

    def set_values(self, feature, value, hashes):
        self.fexists_or_raise(feature)
        table = self.features[feature].table
        column = self.features[feature].column
        default = self.features[feature].default
        values = ', '.join(["('{}', '{}')".format(hash, value) for hash in hashes])
        if not default:
            self.execute('INSERT INTO {tab} (hash, {col}) VALUES {vals} ON CONFLICT(hash, value) DO UPDATE SET value=excluded.value'.format(tab=table, col=column, vals=values))
        else:
            self.execute("INSERT INTO {tab} (hash, {col}) VALUES {vals} ON CONFLICT(hash) DO UPDATE SET {col}=excluded.{col}".format(tab=table, col=column, vals=values))

    def delete_hashes(self, feature, hashes):
        self.fexists_or_raise(feature)
        default = self.features[feature].default
        if not default:
            self.execute("DELETE FROM {} WHERE hash IN ('{}')".format(feature, "', '".join(hashes)))
        else:
            self.set_values(feature, default, hashes)

    def delete_values(self, feature, values):
        self.fexists_or_raise(feature)
        table = self.features[feature].table
        column = self.features[feature].column
        default = self.features[feature].default
        if not default:
            self.execute("DELETE FROM {} WHERE value IN ('{}')".format(feature, "', '".join(values)))
        else:
            self.execute("UPDATE {} SET {} = '{}' WHERE value IN ('{}')".format(table, column, default, "', '".join(values)))


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