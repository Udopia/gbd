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

from gbd.util import eprint
from gbd.schema import Schema, FeatureInfo
from gbd import contexts


class DatabaseException(Exception):
    pass


class Database:

    def __init__(self, path_list, verbose=False):
        self.verbose = verbose
        self.schemas = self.init_schemas(path_list)
        self.features = self.init_features()
        self.connection = sqlite3.connect("file::memory:?cache=shared", uri=True, timeout=10)
        self.cursor = self.connection.cursor()
        self.maindb = None
        schema: Schema
        for schema in self.schemas.values():
            if not schema.is_in_memory():
                self.execute("ATTACH DATABASE '{}' AS {}".format(schema.path, schema.dbname), commit=False)
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
            schema.validate()
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
            for feature in schema.features:
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

    def execute(self, q, commit=True):
        if self.verbose:
            eprint(q)
        self.cursor.execute(q)
        if commit:
            self.connection.commit()

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
        return [ f for (f, i) in self.features.items() if (views and i.virtual) or (tables and not i.virtual) and (not db or db == i.database) ]

    def get_tables(self, context = None, db = None):
        tables = [ i.table for i in self.features.values() if not i.virtual and (not context or context == i.context) and (not db or db == i.database) ]
        return list(set(tables))


    def create_feature(self, name, default_value=None, permissive=False):
        if not permissive:  # internal use can be unchecked, e.g., to create the reserved features during initialization
            Schema.valid_feature_or_raise(name)
        
        if not self.fexists(name):
            # ensure existence of main features-table for context:
            context = contexts.context_from_name(name)
            context_main_table = contexts.prepend_context("features", context)
            context_hash = contexts.prepend_context("hash", context)
            if not context_main_table in self.get_tables():
                self.features[context_hash] = self.schemas[self.maindb].create_main_feature_table(context)

            # create new feature:
            self.execute('ALTER TABLE {}.{} ADD {} TEXT NOT NULL DEFAULT {}'.format(self.maindb, context_main_table, name, default_value or "None"))
            if default_value is not None:
                # feature is unique and resides in main features-table:
                self.features[name] = FeatureInfo(name, self.maindb, context, context_main_table, name, default_value, False)
            else:
                # feature is not unique and resides in a separate table (column in main features-table is a foreign key):
                self.execute("CREATE TABLE IF NOT EXISTS {}.{} (hash TEXT NOT NULL, value TEXT NOT NULL, CONSTRAINT all_unique UNIQUE(hash, value))".format(self.maindb, name))
                self.execute("INSERT INTO {}.{} (hash, value) VALUES ('None', 'None')".format(self.maindb, name))
                self.execute("""CREATE TRIGGER IF NOT EXISTS {}.{}_hash AFTER INSERT ON {}
                                    BEGIN INSERT OR IGNORE INTO {} (hash) VALUES (NEW.hash); END""".format(self.maindb, name, name, context_main_table))
                self.features[name] = FeatureInfo(name, self.maindb, context, name, "value", None, False)

            # update schema:
            self.schemas[self.maindb].features.append(self.features[name])

            # create default filename-views for local path features:
            if name == contexts.prepend_context("local", context):
                self.create_filename_view(context)

        elif not permissive:
            raise DatabaseException("Feature {} already exists".format(name))


    def create_filename_view(self, context):
        local = contexts.prepend_context("local", context)
        filename = contexts.prepend_context("filename", context)
        self.execute("CREATE VIEW IF NOT EXISTS {}.{} (hash, value) AS SELECT hash, REPLACE(value, RTRIM(value, REPLACE(value, '/', '')), '') FROM {}".format(self.maindb, filename, local))


    def rename_feature(self, old_name, new_name):
        Schema.valid_feature_or_raise(new_name)
        self.fexists_or_raise(old_name)
        table = self.features[old_name].table
        self.execute("ALTER TABLE {} RENAME COLUMN {} TO {}".format(table, old_name, new_name))
        if not self.features[old_name].default:
            self.execute("ALTER TABLE {} RENAME TO {}".format(old_name, new_name))
        self.features[new_name] = self.features.pop(old_name)
        self.features[new_name].name = new_name

    def delete_feature(self, name):
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

    def set_values(self, feature, value, hashes):
        self.fexists_or_raise(feature)
        database = self.features[feature].database
        table = self.features[feature].table
        column = self.features[feature].column
        if not self.features[feature].default:
            values = ', '.join(["('{}', '{}')".format(hash, value) for hash in hashes])
            self.execute("INSERT OR IGNORE INTO {d}.{tab} (hash, {col}) VALUES {vals}".format(d=database, tab=table, col=column, vals=values))
            self.execute("UPDATE {d}.{tab} SET {col}=hash WHERE hash in ('{h}')".format(d=database, tab="features", col=table, h="', '".join(hashes)))
        else:
            #self.execute("UPDATE {d}.{tab} SET {col}='{val}' WHERE hash IN ('{h}')".format(d=database, tab=table, col=column, val=value, h="', '".join(hashes)))
            self.execute("INSERT INTO {d}.{tab} (hash, {col}) VALUES {vals} ON CONFLICT (hash) DO UPDATE SET {col}='{val}' WHERE hash in ('{h}')".format(d=database, tab=table, col=column, val=value, vals=', '.join(["('{}', '{}')".format(hash, value) for hash in hashes]), h="', '".join(hashes)))

    def delete(self, feature, values=[], hashes=[]):
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