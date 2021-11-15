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
import json
import csv
import itertools
import collections
from sqlite3.dbapi2 import IntegrityError

import gbd_tool.config as config
from gbd_tool.gbd_hash import HASH_VERSION
from gbd_tool.util import eprint, prepend_context, context_from_name, make_alnum_ul

VERSION = 0


class DatabaseException(Exception):
    pass


class Database:
    def __init__(self, path_list, verbose=False):
        self.database_infos = collections.OrderedDict()
        self.feature_infos = dict()
        self.verbose = verbose
        self.connection = sqlite3.connect("file::memory:?cache=shared", uri=True)
        self.cursor = self.connection.cursor()
        # init non-existent databases and check existing databases
        have_csv = False
        for path in path_list:
            if not os.path.isfile(path):
                self.create(path, VERSION, HASH_VERSION)
                self.extract_database_infos(path)
            elif self.is_database(path):
                self.check(path, VERSION, HASH_VERSION)
                self.extract_database_infos(path)
            else:
                self.import_csv(path)
                have_csv = True
        if have_csv:
            self.extract_database_infos("file::memory:?cache=shared", all_virtual=True)
        for info in self.database_infos.values():
            self.init_database(info)
            self.cursor.execute("ATTACH DATABASE '{}' AS {}".format(info['path'], info['name']))
        # self.migrate()
        
    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.connection.commit()
        self.connection.close()

    def is_database(self, db):
        if not os.path.isfile(db): return False
        sz = os.path.getsize(db)
        if sz == 0: return True  # New sqlite3 files created in recent libraries are empty
        if sz < 100: return False  # SQLite database file header is 100 bytes
        with open(db, 'rb') as fd: header = fd.read(100)  # Validate file header
        return (header[:16] == b'SQLite format 3\x00')

    def import_csv(self, csvfile):        
        file = os.path.basename(csvfile)
        name = "{}_{}".format(context_from_name(file), make_alnum_ul(file))
        cur = self.inmemory.cursor()
        if not len(cur.execute("SELECT * FROM sqlite_master WHERE tbl_name='{}'".format(name)).fetchall()):
            with open(csvfile) as csvfile:
                csvreader = csv.DictReader(csvfile)
                if not "hash" in csvreader.fieldnames:
                    raise DatabaseException("Column 'hash' not found in {}".format(csvfile))
                cur.execute('CREATE TABLE IF NOT EXISTS {} ({})'.format(name, ", ".join(csvreader.fieldnames)))
                for row in csvreader:
                    cur.execute("INSERT INTO {} VALUES ('{}')".format(name, "', '".join(row.values())))
            self.inmemory.commit()

    def create(self, path, version, hash_version):
        eprint("Warning: Creating Database {}".format(path))
        con = sqlite3.connect(path)
        con.cursor().execute("CREATE TABLE __version (version INT, hash_version INT)")
        con.cursor().execute("INSERT INTO __version (version, hash_version) VALUES ({}, {})".format(version, hash_version))
        con.cursor().execute("CREATE TABLE IF NOT EXISTS __meta (name TEXT UNIQUE, value BLOB)")
        con.commit()
        con.close()

    def check(self, path, version, hash_version):
        __version = sqlite3.connect(path).execute("SELECT version, hash_version FROM __version").fetchall()
        if __version[0][0] != version:
            eprint("WARNING: DB Version is {} but tool version is {}".format(__version[0][0], version))
        if __version[0][1] != hash_version:
            eprint("WARNING: DB Hash-Version is {} but tool hash-version is {}".format(__version[0][1], hash_version))

    def extract_database_infos(self, path, all_virtual=False):
        dbname = make_alnum_ul(os.path.basename(path))
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
            self.extract_feature_infos(path, table, context, dbname, all_virtual)

    def extract_feature_infos(self, path, table, context, dbname, all_virtual=False):
        infos = sqlite3.connect(path).execute("PRAGMA table_info({})".format(table)).fetchall()
        names = ('index', 'name', 'type', 'notnull', 'default_value', 'pk')
        for info in infos:
            col = dict(zip(names, info))
            if col['name'] != "hash":
                fname = table if col['name'] == "value" else col['name']
                if fname in self.feature_infos:
                    eprint("Warning: Feature {f} is in {d} and {e}. Using {e}.".format(f=fname, d=dbname, e=self.feature_infos[fname]['database']))
                else:
                    self.feature_infos[fname] = { "table": "{}.{}".format(dbname, table), 
                        "column": col['name'], "default": col['default_value'], 
                        "virtual": (type == 'view' or all_virtual), 
                        "context": context, "database": dbname
                    }

    # migrate to new data model
    def migrate(self):
        try:
            self.execute('INSERT INTO features (hash) SELECT DISTINCT(hash) FROM local')
        except IntegrityError as err:
            eprint(str(err))
        for feature in self.features():
            if self.fdefault(feature) and self.fcolumn(feature) == "value" and not feature.startswith("migrate_"):
                self.execute('DROP TRIGGER IF EXISTS {}_dval'.format(feature))
                self.execute('DROP TRIGGER IF EXISTS {}_unique'.format(feature))
                temp = "migrate_" + feature
                self.rename_feature(feature, temp)
                self.execute('ALTER TABLE features ADD {} TEXT NOT NULL DEFAULT {}'.format(feature, self.fdefault(feature)))
                self.execute('''UPDATE features SET {n} = (SELECT {m}.value FROM {m} WHERE features.hash = {m}.hash AND {m}.value IS NOT NULL)
                                WHERE hash IN (SELECT hash FROM {m} WHERE {m}.hash = features.hash)'''.format(n=feature, m=temp))
                self.execute('DROP TABLE {}'.format(temp))
                self.feature_infos[feature]['table'] = "features"
                self.feature_infos[feature]['column'] = feature

    def init_database(self, dbinfo):
        # existence of local, filename and hash
        for context in dbinfo['contexts']:
            local = prepend_context("local", context)
            filename = prepend_context("filename", context)
            hashv = prepend_context("hash", context)
            features = prepend_context("features", context)
            if not local in dbinfo['tables']:
                sqlite3.connect(dbinfo['path']).execute("CREATE TABLE {} (hash TEXT NOT NULL, value TEXT NOT NULL)".format(local))
            if not filename in dbinfo['views']:
                sqlite3.connect(dbinfo['path']).execute("CREATE VIEW {} (hash, value) AS SELECT hash, REPLACE(value, RTRIM(value, REPLACE(value, '/', '')), '') FROM {}".format(filename, context))
            if not hashv in dbinfo['views']:
                sqlite3.connect(dbinfo['path']).execute("CREATE VIEW {} (hash, value) AS SELECT DISTINCT hash, hash FROM {}".format(hashv, local))
            if not features in dbinfo['tables']:
                sqlite3.connect(dbinfo['path']).execute("CREATE TABLE {} (hash UNIQUE NOT NULL)".format(features))
                sqlite3.connect(dbinfo['path']).execute("INSERT OR IGNORE INTO {} (hash) SELECT DISTINCT(hash) FROM {}".format(features, local))
                sqlite3.connect(dbinfo['path']).execute('''CREATE TRIGGER {}_dval AFTER INSERT ON {} BEGIN INSERT OR IGNORE INTO {} (hash) VALUES (NEW.hash); END'''.format(features, local, features))
            
        # existence of context translators
        for (c0, c1) in list(itertools.permutations(dbinfo["contexts"], 2)):
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

    def features(self):
        return self.feature_infos.keys()

    def valid_fname_or_raise(self, name):
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
        if default_value is not None:
            features = prepend_context("features", context_from_name(name))
            self.execute('ALTER TABLE {} ADD {} TEXT NOT NULL DEFAULT {}'.format(features, name, default_value))
        else:
            self.execute('CREATE TABLE IF NOT EXISTS {} (hash TEXT NOT NULL, value TEXT NOT NULL, CONSTRAINT all_unique UNIQUE(hash, value))'.format(name))

    def rename_feature(self, old_name, new_name):
        self.valid_fname_or_raise(new_name)
        info = self.feature_infos[old_name]
        if info["column"] == "value":
            self.execute("ALTER TABLE {} RENAME TO {}".format(old_name, new_name))
            self.execute("UPDATE __meta SET name='{}' WHERE name='{}'".format(new_name, old_name))
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
        if info["column"] == "value" and not info['default']:
            values = ', '.join(["('{}', '{}')".format(hash, value) for hash in hashes])
            self.execute('INSERT INTO {} (hash, {}) VALUES {}'.format(info['table'], info['column'], values))
        else:
            self.execute("UPDATE {} SET {}='{}' WHERE hash IN ('{}')".format(info['table'], info['column'], value, "', '".join(hashes)))


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

    def meta_record(self, table):
        blob = self.query("SELECT value FROM __meta WHERE name = '{}'".format(table))
        return json.loads(blob or "{}")

    def meta_clear(self, table, meta_feature=None):
        if not meta_feature:
            self.execute("REPLACE INTO __meta (name, value) VALUES ('{}', '')".format(table))
        else:
            values = self.meta_record(table)
            if meta_feature in values:
                values.pop(meta_feature)
            self.execute("REPLACE INTO __meta (name, value) VALUES ('{}', '{}')".format(table, json.dumps(values)))

    def meta_set(self, table, meta_feature, value):
        values = self.meta_record(table)
        values[meta_feature] = value
        self.execute("REPLACE INTO __meta (name, value) VALUES ('{}', '{}')".format(table, json.dumps(values)))
