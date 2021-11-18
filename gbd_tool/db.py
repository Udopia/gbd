# GBD Benchmark Database (GBD)
# Copyright (C) 2020 Markus Iser, Karlsruhe Institute of Technology (KIT)
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
import os
import csv
import itertools
import collections

import gbd_tool.config as config
from gbd_tool.gbd_hash import HASH_VERSION
from gbd_tool.util import eprint, prepend_context, context_from_name, make_alnum_ul

VERSION = 0


class DatabaseException(Exception):
    pass


class Database:
    INMEMORYDB = "file::memory:?cache=shared"

    def __init__(self, path_list, verbose=False):
        self.database_infos = collections.OrderedDict()
        self.feature_infos = dict()
        self.verbose = verbose
        self.connection = sqlite3.connect(self.INMEMORYDB, uri=True)
        self.cursor = self.connection.cursor()
        # init non-existent databases and check existing databases
        for path in path_list:
            if not os.path.isfile(path):
                self.create(path, VERSION, HASH_VERSION)
                self.extract_database_infos(path)
            elif self.is_database(path):
                self.check(path, VERSION, HASH_VERSION)
                self.extract_database_infos(path)
            else:
                self.import_csv(path)
        for info in self.database_infos.values():
            self.cursor.execute("ATTACH DATABASE '{}' AS {}".format(info['path'], info['name']))
            if info['main']:
                self.maindb = info['name']  # target of inserts and updates
        self.extract_database_infos(self.INMEMORYDB, virtual=True)
        for dbname in self.database_infos.keys():
            self.create_feature_tables(dbname)
            self.create_context_translators(dbname, self.dcontexts(dbname))
        
    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.connection.commit()
        self.connection.close()

    def is_database(self, db):
        if not os.path.isfile(db): return False
        sz = os.path.getsize(db)
        if sz == 0: return True  # new sqlite3 files can be empty
        if sz < 100: return False  # sqlite header is 100 bytes
        with open(db, 'rb') as fd: header = fd.read(100)  # validate header
        return (header[:16] == b'SQLite format 3\x00')

    def import_csv(self, csvfile):        
        file = os.path.basename(csvfile)
        name = "{}_{}".format(context_from_name(file), make_alnum_ul(file))
        cur = self.connection.cursor()
        if not len(cur.execute("SELECT * FROM sqlite_master WHERE tbl_name='{}'".format(name)).fetchall()):
            with open(csvfile) as csvfile:
                csvreader = csv.DictReader(csvfile)
                if not "hash" in csvreader.fieldnames:
                    raise DatabaseException("Column 'hash' not found in {}".format(csvfile))
                cur.execute('CREATE TABLE IF NOT EXISTS {} ({})'.format(name, ", ".join(csvreader.fieldnames)))
                for row in csvreader:
                    cur.execute("INSERT INTO {} VALUES ('{}')".format(name, "', '".join(row.values())))
            self.connection.commit()

    def create(self, path, version, hash_version):
        eprint("Warning: Creating Database {}".format(path))
        con = sqlite3.connect(path)
        con.cursor().execute("CREATE TABLE __version (version INT, hash_version INT)")
        con.cursor().execute("INSERT INTO __version (version, hash_version) VALUES ({}, {})".format(version, hash_version))
        con.commit()
        con.close()

    def check(self, path, version, hash_version):
        __version = sqlite3.connect(path).execute("SELECT version, hash_version FROM __version").fetchall()
        if __version[0][0] != version:
            eprint("WARNING: DB Version is {} but tool version is {}".format(__version[0][0], version))
        if __version[0][1] != hash_version:
            eprint("WARNING: DB Hash-Version is {} but tool hash-version is {}".format(__version[0][1], hash_version))

    def extract_database_infos(self, path, virtual=False):
        dbname = "main" if path == self.INMEMORYDB else make_alnum_ul(os.path.basename(path))
        firstdb = len(self.database_infos) == 0
        self.database_infos[dbname] = { 'name': dbname, 'path': path, 'main': firstdb, 'contexts': [], 'tables': [], 'views': [] }
        sql_tables="""SELECT tbl_name, type FROM sqlite_master WHERE type IN ('table', 'view') 
                        AND NOT tbl_name LIKE 'sqlite$_%' ESCAPE '$' AND NOT tbl_name LIKE '$_$_%' ESCAPE '$'"""
        tables = sqlite3.connect(path).execute(sql_tables).fetchall()
        for (table, type) in tables:
            context = context_from_name(table)
            if not context in self.database_infos[dbname]['contexts']:
                self.database_infos[dbname]['contexts'].append(context)
            self.database_infos[dbname][type + 's'].append(table)
            self.extract_feature_infos(path, table, context, dbname, virtual or type == "view")

    def extract_feature_infos(self, path, table, context, dbname, virtual=False):
        infos = sqlite3.connect(path).execute("PRAGMA table_info({})".format(table)).fetchall()
        names = ('index', 'name', 'type', 'notnull', 'default_value', 'pk')
        for info in infos:
            col = dict(zip(names, info))
            if col['name'] != "hash":
                fname = table if col['name'] == "value" else col['name']
                if fname in self.feature_infos.keys():
                    eprint("Warning: Feature {f} is in {d} and {e}. Using {e}.".format(f=fname, d=dbname, e=self.feature_infos[fname]['database']))
                else:
                    self.feature_infos[fname] = { "table": "{}.{}".format(dbname, table), 
                        "column": col['name'], "default": col['default_value'], 
                        "virtual": virtual, "context": context, "database": dbname
                    }

    def create_feature_tables(self, dbname, contexts=None):
        if not contexts:
            contexts = self.dcontexts(dbname)
        for context in contexts:
            features = "{}.{}".format(dbname, prepend_context("features", context))
            tables = self.tables(context=context, dbname=dbname)
            if not features in tables:
                self.execute("CREATE TABLE IF NOT EXISTS {} (hash UNIQUE NOT NULL)".format(features))
                for table in tables:
                    self.execute("INSERT OR IGNORE INTO {} (hash) SELECT DISTINCT(hash) FROM {}".format(prepend_context("features", context), table))
                    self.execute("CREATE TRIGGER IF NOT EXISTS {}_dval AFTER INSERT ON {} BEGIN INSERT OR IGNORE INTO {} (hash) VALUES (NEW.hash); END".format(table, table, prepend_context("features", context)))
                self.database_infos[dbname]['tables'].append(prepend_context("features", context))
            hashv = prepend_context("hash", context)
            if not hashv in self.feature_infos.keys():
                self.feature_infos[hashv] = { "table": features, "column": hashv, "default": True, "virtual": True, "context": context, "database": dbname }

    def create_context_translators(self, dbname, contexts):
        dbinfo = self.database_infos[dbname]
        for (c0, c1) in list(itertools.permutations(contexts, 2)):
            translator = "translator_{}_{}".format(c0, c1)
            if not translator in dbinfo['tables']:
                sqlite3.connect(dbinfo['path']).execute("CREATE TABLE {} (hash, value)".format(translator))

    def dpath(self, dbname):
        return self.database_infos[dbname]['path']
        
    def dmain(self, dbname):
        return self.database_infos[dbname]['main']
        
    def dcontexts(self, dbname):
        return self.database_infos[dbname]['contexts']
        
    def dtables(self, dbname):
        return self.database_infos[dbname]['tables']
        
    def dviews(self, dbname):
        return self.database_infos[dbname]['views']

    def ftable(self, feature):
        return self.feature_infos[feature]['table']

    def fcolumn(self, feature):
        return self.feature_infos[feature]['column']

    def fdefault(self, feature):
        return self.feature_infos[feature]['default']

    def fvirtual(self, feature):
        return self.feature_infos[feature]['virtual']

    def fcontext(self, feature):
        return self.feature_infos[feature]['context']

    def fdatabase(self, feature):
        return self.feature_infos[feature]['database']

    def databases(self):
        return self.database_infos.keys()
  
    def features(self, tables=True, views=False, database=None):
        result = []
        for (feature, info) in self.feature_infos.items():
            if not views and info["virtual"]:
                continue
            if not tables and not info["virtual"]:
                continue
            if database and database != info["database"]:
                continue
            result.append(feature)
        return result

    def tables(self, context = None, dbname = None):
        tables = list()
        for finfo in self.feature_infos.values():
            if (not context or context == finfo['context']) and (not dbname or dbname == finfo['database']) and not finfo['virtual']:
                if not finfo['table'] in tables:
                    tables.append(finfo['table'])
        return tables

    def valid_fname_or_raise(self, name):
        if len(name) < 2:
            raise DatabaseException("Feature name '{}' to short.".format(name))
        if name in self.features():
            raise DatabaseException("Feature '{}' exists.".format(name))
        reserved = [ 'hash', 'value', 'local', 'filename', 'features' ]
        if name.lower() in reserved or name.lower() in config.contexts() or name.startswith("__"):
            raise DatabaseException("'{}' is reserved.".format(name))
        sqlite_keywords = ['abort', 'action', 'add', 'after', 'all', 'alter', 'always', 'analyze', 'and', 'as', 'asc', 'attach', 'autoincrement', 
            'before', 'begin', 'between', 'by', 'cascade', 'case', 'cast', 'check', 'collate', 'column', 'commit', 'conflict', 'constraint', 
            'create', 'cross', 'current', 'current_date', 'current_time', 'current_timestamp', 'database', 'default', 'deferrable', 'deferred', 
            'delete', 'desc', 'detach', 'distinct', 'do', 'drop', 'each', 'else', 'end', 'escape', 'except', 'exclude', 'exclusive', 'exists', 
            'explain', 'fail', 'filter', 'first', 'following', 'for', 'foreign', 'from', 'full', 'generated', 'glob', 'group', 'groups', 
            'having', 'if', 'ignore', 'immediate', 'in', 'index', 'indexed', 'initially', 'inner', 'insert', 'instead', 'intersect', 'into', 'is', 'isnull', 
            'join', 'key', 'last', 'left', 'like', 'limit', 'match', 'materialized', 'natural', 'no', 'not', 'nothing', 'notnull', 'null', 'nulls', 
            'of', 'offset', 'on', 'or', 'order', 'others', 'outer', 'over', 'partition', 'plan', 'pragma', 'preceding', 'primary', 'query', 
            'raise', 'range', 'recursive', 'references', 'regexp', 'reindex', 'release', 'rename', 'replace', 'restrict', 'returning', 'right', 'rollback', 
            'row', 'rows', 'savepoint', 'select', 'set', 'table', 'temp', 'temporary', 'then', 'ties', 'to', 'transaction', 'trigger', 'unbounded', 'union', 
            'unique', 'update', 'using', 'vacuum', 'values', 'view', 'virtual', 'when', 'where', 'window', 'with', 'without']
        if name.lower() in sqlite_keywords or name.startswith("sqlite_"):
            raise DatabaseException("'{}' is reserved by sqlite.".format(name))

    def create_feature(self, name, default_value=None):
        self.valid_fname_or_raise(name)
        context = context_from_name(name)
        if default_value is not None:
            features = prepend_context("features", context)
            #if not features in self.database_infos[self.maindb]['tables']:
            self.create_feature_tables(self.maindb, [context_from_name(name)])
            self.execute('ALTER TABLE {}.{} ADD {} TEXT NOT NULL DEFAULT {}'.format(self.maindb, features, name, default_value))
            # update info
            if not context in self.database_infos[self.maindb]['contexts']:
                self.database_infos[self.maindb]['contexts'].append(context)
            self.feature_infos[name] = { "table": "{}.{}".format(self.maindb, features), "column": name, "default": default_value, "virtual": False, "context": context, "database": self.maindb }
        else:
            self.execute('CREATE TABLE IF NOT EXISTS {}.{} (hash TEXT NOT NULL, value TEXT NOT NULL, CONSTRAINT all_unique UNIQUE(hash, value))'.format(self.maindb, name))
            # insert default values for new hashes into features table
            features = prepend_context("features", context)
            self.execute("CREATE TRIGGER IF NOT EXISTS {}.{}_dval AFTER INSERT ON {} BEGIN INSERT OR IGNORE INTO {} (hash) VALUES (NEW.hash); END".format(self.maindb, name, name, features))
            # create "filename" view for "local" tables
            if name == context + "_local":
                filename = prepend_context("filename", context)
                self.execute("CREATE VIEW IF NOT EXISTS {}.{} (hash, value) AS SELECT hash, REPLACE(value, RTRIM(value, REPLACE(value, '/', '')), '') FROM {}".format(self.maindb, filename, name))
            # update info
            if not context in self.database_infos[self.maindb]['contexts']:
                self.database_infos[self.maindb]['contexts'].append(context)
            self.database_infos[self.maindb]["tables"].append(name)
            self.feature_infos[name] = { "table": "{}.{}".format(self.maindb, name), "column": "value", "default": None, "virtual": False, "context": context, "database": self.maindb }

    def rename_feature(self, old_name, new_name):
        self.valid_fname_or_raise(new_name)
        info = self.feature_infos[old_name]
        if info["column"] == "value":
            self.execute("ALTER TABLE {} RENAME TO {}".format(old_name, new_name))
        else:
            raise DatabaseException("Not Implemented")

    def delete_feature(self, name):
        info = self.feature_infos[name]
        if info["column"] == "value":
            self.execute('DROP TABLE IF EXISTS {}'.format(name))
            self.execute('DROP TRIGGER IF EXISTS {}_dval'.format(name))
            self.execute('DROP TRIGGER IF EXISTS {}_unique'.format(name))
        else:
            raise DatabaseException("Not Implemented")

    def insert(self, feature, value, hashes):
        info = self.feature_infos[feature]
        values = ', '.join(["('{}', '{}')".format(hash, value) for hash in hashes])
        self.execute('INSERT or REPLACE INTO {tab} (hash, {col}) VALUES {vals}'.format(tab=info['table'], col=info['column'], vals=values))
        #self.execute('INSERT INTO {tab} (hash, {col}) VALUES {vals} ON CONFLICT(hash) DO UPDATE SET {col}=excluded.{col}'.format(tab=info['table'], col=info['column'], vals=values))

    def delete_hashes(self, feature, hashes):
        info = self.feature_infos[feature]
        if info['default'] is None:
            self.execute("DELETE FROM {} WHERE hash IN ('{}')".format(feature, "', '".join(hashes)))
        else:
            self.insert(feature, info['default'], hashes)

    def delete_values(self, feature, values):
        info = self.feature_infos[feature]
        if info['default'] is None:
            self.execute("DELETE FROM {} WHERE value IN ('{}')".format(feature, "', '".join(values)))
        else:
            self.execute("UPDATE {} SET {} = {} WHERE value IN ('{}')".format(info['table'], info['column'], info['default'], "', '".join(values)))

    def query(self, q):
        if self.verbose:
            eprint(q)
        return self.cursor.execute(q).fetchall()

    def execute(self, q):
        if self.verbose:
            eprint(q)
        self.cursor.execute(q)

    # return list of distinct values and value-range for numeric values
    def feature_values(self, feature):
        table = self.feature_infos[feature]['table']
        column = self.feature_infos[feature]['column']
        result = { "numeric" : [None, None], "discrete" : [] }
        minmax = self.query('SELECT MIN(CAST({col} AS NUMERIC)), MAX(CAST({col} AS NUMERIC)) FROM {tab} WHERE NOT {col} GLOB "*[^0-9.e\-]*" AND {col} LIKE "_%"'.format(col=column, tab=table))
        if len(minmax):
            result['numeric'] = minmax[0]
        records = self.query('SELECT DISTINCT {col} FROM {tab} WHERE {col} GLOB "*[^0-9.e\-]*" OR {col} NOT LIKE "_%"'.format(col=column, tab=table))
        result["discrete"] = [x[0] for x in records]
        return result

    def system_record(self, feature):
        system_record = dict()
        system_record['feature_name'] = feature
        system_record['feature_count'] = self.query('SELECT COUNT(*) FROM {}'.format(self.feature_infos[feature]['table']))[0][0]
        system_record['feature_default'] = self.feature_infos[feature]["default"]
        values = self.feature_values(feature)
        system_record['feature_min'] = values['numeric'][0]
        system_record['feature_max'] = values['numeric'][1]
        system_record['feature_values'] = " ".join(values['discrete'])
        return system_record