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
            # first database is the default database:
            if not self.maindb:
                self.maindb = schema.dbname
        
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


    # return a dictionary which maps feature names to feature infos
    def init_features(self) -> typing.Dict[str, FeatureInfo]:
        result = dict()
        schema: Schema
        for schema in self.schemas.values():
            feature: FeatureInfo
            for feature in schema.features.values():
                # first found feature is used: (=feature precedence by database position)
                if not feature.name in result:
                    result[feature.name] = [ feature ]
                elif feature.column == "hash" and feature.table == "features": 
                    # first found features table is the one that serves the hash
                    if result[feature.name][0].table != "features":
                        result[feature.name].insert(0, feature)
                else:
                    result[feature.name].append(feature)
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
        
    def dcontext(self, dbname):
        return self.schemas[dbname].context
        
    def dtables(self, dbname):
        return self.schemas[dbname].get_tables()
        
    def dviews(self, dbname):
        return self.schemas[dbname].get_views()


    def finfos(self, fname, db=None):
        if not self.fexists(fname):
            return [ ]
        return [ info for info in self.features[fname] if (db is None or info.database == db) ]

    def finfo(self, fname, db=None):
        infos = self.finfos(fname, db)
        if len(infos) == 0:
            raise DatabaseException("Feature {} does not exists in database {}".format(fname, db))
        return infos[0]

    def fexists(self, fname):
        return fname in self.features.keys() and len(self.features[fname]) > 0


    def get_databases(self):
        return self.schemas.keys()
  
    def get_features(self, tables=True, views=True, db=None):
        return [ name for (name, infos) in self.features.items() for info in infos if ((views and info.virtual) or (tables and not info.virtual)) and (not db or db == info.database) ]

    def get_tables(self, db=None):
        tables = [ info.table for infos in self.features.values() for info in infos if not info.virtual and (not db or db == info.database) ]
        return list(set(tables))


    def create_feature(self, name, default_value=None, target_db=None, permissive=False):
        db = target_db or self.maindb
        created = self.schemas[db].create_feature(name, default_value, permissive)
        for finfo in created:
            if not finfo.name in self.features.keys():
                self.features[finfo.name] = [ finfo ]
            else:
                # this code disregards feature precedence by database position:
                self.features[finfo.name].append(finfo)


    def set_values(self, fname, value, hashes, target_db=None):
        finfo = self.finfo(fname)
        self.schemas[finfo.database].set_values(fname, value, hashes)


    def rename_feature(self, fname, new_fname, target_db=None):
        Schema.valid_feature_or_raise(new_fname)
        finfo = self.finfo(fname, target_db)
        self.execute("ALTER TABLE {}.{} RENAME COLUMN {} TO {}".format(finfo.database, finfo.table, fname, new_fname))
        if not finfo.default:
            self.execute("ALTER TABLE {}.{} RENAME TO {}.{}".format(finfo.database, fname, finfo.database, new_fname))
        self.features[fname].remove(finfo)
        finfo.name = new_fname
        if not new_fname in self.features.keys():
            self.features[new_fname] = [ finfo ]
        else:
            # this code disregards feature precedence by database position:
            self.features[new_fname].append(finfo)


    def delete_feature(self, fname, target_db=None):
        finfo = self.finfo(fname, target_db)
        if not finfo.default:
            self.execute('DROP TABLE IF EXISTS {}.{}'.format(finfo.database, fname))
            if fname == "local":
                self.execute('DROP VIEW IF EXISTS {}.filename'.format(finfo.database))
        elif Database.sqlite3_version() >= 3.35:
            self.execute("ALTER TABLE {}.{} DROP COLUMN {}".format(finfo.database, finfo.table, fname))
        else:
            raise DatabaseException("Cannot delete unique feature {} with SQLite versions < 3.35".format(fname))
        self.features[fname].remove(finfo)


    def delete(self, fname, values=[], hashes=[], target_db=None):
        finfo = self.finfo(fname, target_db)
        w1 = "{col} IN ('{v}')".format(col=finfo.column, v="', '".join(values)) if len(values) else "1=1"
        w2 = "hash IN ('{h}')".format(h="', '".join(hashes)) if len(hashes) else "1=1"
        where = "{} AND {}".format(w1, w2)
        db = finfo.database
        if not finfo.default:
            hashlist = [ r[0] for r in self.query("SELECT DISTINCT(hash) FROM {d}.{tab} WHERE {w}".format(d=db, tab=fname, w=where)) ]
            self.execute("DELETE FROM {d}.{tab} WHERE {w}".format(d=db, tab=fname, w=where))
            remaining = [ r[0] for r in self.query("SELECT DISTINCT(hash) FROM {d}.{tab} WHERE hash in ('{h}')".format(d=db, tab=fname, h="', '".join(hashlist))) ]
            setnone = [ h for h in hashlist if not h in remaining ]
            self.execute("UPDATE {d}.{tab} SET {col} = 'None' WHERE hash IN ('{h}')".format(d=db, tab="features", col=fname, h="', '".join(setnone)))
        else:
            self.execute("UPDATE {d}.{tab} SET {col} = '{default}' WHERE {w}".format(d=db, tab="features", col=fname, default=finfo.default, w=where))
