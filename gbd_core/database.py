
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
import typing

from pprint import pprint

from gbd_core.util import eprint
from gbd_core.schema import Schema, FeatureInfo
from gbd_core import contexts


class DatabaseException(Exception):
    pass


class Database:

    def __init__(self, path_list: list, verbose=False, autocommit=True):
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
            else:
                self.execute("ATTACH DATABASE 'file:{}?mode=memory&cache=shared' AS {}".format(schema.dbname, schema.dbname))
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
        if not dbname in self.schemas:
            raise DatabaseException("Database '{}' not found".format(dbname))
        return self.schemas[dbname].path
        
    def dcontext(self, dbname):
        if not dbname in self.schemas:
            raise DatabaseException("Database '{}' not found".format(dbname))
        return self.schemas[dbname].context
        
    def dtables(self, dbname):
        if not dbname in self.schemas:
            raise DatabaseException("Database '{}' not found".format(dbname))
        return self.schemas[dbname].get_tables()


    def finfo(self, fname, db=None):
        if fname in self.features and len(self.features[fname]) > 0:
            if db is None:
                return self.features[fname][0]
            else:
                infos = [ info for info in self.features[fname] if info.database == db ]
                if len(infos) == 0:
                    raise DatabaseException("Feature '{}' does not exists in database {}".format(fname, db))
                return infos[0]
        else:
            raise DatabaseException("Feature '{}' does not exists".format(fname))
        

    def faddr_column(self, feature):
        finfo = self.find(feature)
        return "{}.{}.{}".format(finfo.database, finfo.table, finfo.column)

    def faddr_table(self, feature):
        finfo = self.find(feature)
        return "{}.{}".format(finfo.database, finfo.table)
    

    def find(self, fid: str, db: str=None):
        """ Find feature by name or feature identifier
        
            Args:
            fid: feature identifier, of the form "database:feature", "context:feature" or "feature"
            db: database name (optional), if given fid is unique without database: or context: prefix

            Returns:
            FeatureInfo object: the info object for the first found feature
            feature precedence is according to the order of databases in the path list
            ambiguity can be resolved by using one of the following methods.
            - by giving a database name as the second argument or
            - by using the fid syntax "database:feature"
            - by using the fid syntax "context:feature" (note that this does not necessarily resolve all ambiguity)

            Raises:
            DatabaseException: if feature is not found or given database info is ambiguous
        """
        parts = fid.split(":")
        if db is not None:
            if len(parts) > 1:
                if parts[0] != db:
                    raise DatabaseException("Ambiguous database identifiers: '{}' and '{}'".format(parts[0], db))
                else:
                    return self.finfo(parts[1], parts[0])
            return self.finfo(fid, db)
        elif len(parts) == 1:
            return self.finfo(fid)
        elif parts[0] in self.get_databases():
            return self.finfo(parts[1], parts[0])
        elif parts[0] in self.get_contexts():
            db = self.get_databases(parts[0])[0]
            return self.finfo(parts[1], db)
        else:
            raise DatabaseException("Feature '{}' not found".format(fid))
        

    def faddr(self, fid: str, with_column=True):
        finfo = self.find(fid)

        if with_column:
            return "{}.{}.{}".format(finfo.database, finfo.table, finfo.column)
        else:
            return "{}.{}".format(finfo.database, finfo.table)


    def get_databases(self, context: str=None):
        return [ dbname for (dbname, schema) in self.schemas.items() if not context or context == schema.context ]

    def get_contexts(self, dbs=[]):
        return list(set([ s.context for s in self.schemas.values() if not dbs or s.dbname in dbs ]))
  
    def get_features(self, dbs=[]):
        return [ name for (name, infos) in self.features.items() for info in infos if not dbs or info.database in dbs ]

    def get_tables(self, dbs=[]):
        tables = [ info.table for infos in self.features.values() for info in infos if not dbs or info.database in dbs ]
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
        finfo = self.finfo(fname, target_db)
        self.schemas[finfo.database].set_values(fname, value, hashes)


    def rename_feature(self, fname, new_fname, target_db=None):
        Schema.valid_feature_or_raise(new_fname)
        finfo = self.finfo(fname, target_db)
        self.execute("ALTER TABLE {}.features RENAME COLUMN {} TO {}".format(finfo.database, fname, new_fname))
        if finfo.default is None:
            con = sqlite3.connect(self.schemas[finfo.database].path)
            with con as cursor:
                cursor.execute("ALTER TABLE {} RENAME TO {}".format(fname, new_fname))
            con.close()
        self.features[fname].remove(finfo)
        if not len(self.features[fname]):
            del self.features[fname]
        finfo.name = new_fname
        if not new_fname in self.features.keys():
            self.features[new_fname] = [ finfo ]
        else:
            # this code disregards feature precedence by database position:
            self.features[new_fname].append(finfo)


    def delete_feature(self, fname, target_db=None):
        finfo = self.finfo(fname, target_db)
        if finfo.default is None:
            self.execute('DROP TABLE IF EXISTS {}.{}'.format(finfo.database, fname))
        elif Database.sqlite3_version() >= 3.35:
            self.execute("ALTER TABLE {}.{} DROP COLUMN {}".format(finfo.database, finfo.table, fname))
        else:
            raise DatabaseException("Cannot delete unique feature {} with SQLite versions < 3.35".format(fname))
        self.features[fname].remove(finfo)
        if not len(self.features[fname]):
            del self.features[fname]


    def delete(self, fname, values=[], hashes=[], target_db=None):
        finfo = self.finfo(fname, target_db)
        w1 = "{cl} IN ('{v}')".format(cl=finfo.column, v="', '".join(values))
        w2 = "hash IN ('{h}')".format(h="', '".join(hashes))
        where = "{} AND {}".format(w1 if len(values) else "1=1", w2 if len(hashes) else "1=1")
        db = finfo.database
        if finfo.default is None:
            hashlist = [ r[0] for r in self.query("SELECT DISTINCT(hash) FROM {d}.{tab} WHERE {w}".format(d=db, tab=fname, w=where)) ]
            self.execute("DELETE FROM {d}.{tab} WHERE {w}".format(d=db, tab=fname, w=where))
            remaining = [ r[0] for r in self.query("SELECT DISTINCT(hash) FROM {d}.{tab} WHERE hash in ('{h}')".format(d=db, tab=fname, h="', '".join(hashlist))) ]
            setnone = [ h for h in hashlist if not h in remaining ]
            self.execute("UPDATE {d}.features SET {col} = 'None' WHERE hash IN ('{h}')".format(d=db, col=fname, h="', '".join(setnone)))
        else:
            self.execute("UPDATE {d}.features SET {col} = '{default}' WHERE {w}".format(d=db, col=fname, default=finfo.default, w=where))


    def delete_hashes_entirely(self, hashes, target_db=None):
        tables = self.get_tables([ target_db ])
        for table in tables:
            self.execute("DELETE FROM {}.{} WHERE hash IN ('{h}')".format(target_db, table, h="', '".join(hashes)))


    def copy_feature(self, old_name, new_name, target_db, hashlist=[]):
        old_finfo = self.find(old_name)
        data = self.query("SELECT hash, {col} FROM {d}.{tab} WHERE hash IN ('{h}')".format(d=old_finfo.database, col=old_finfo.column, tab=old_finfo.table, h="', '".join(hashlist)))
        for (hash, value) in data:
            self.set_values(new_name, value, [hash], target_db)

