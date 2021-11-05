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
import re
import json
import csv
import itertools

import gbd_tool.config as config
from gbd_tool.gbd_hash import HASH_VERSION
from gbd_tool.util import eprint, is_number, prepend_context, context_from_name

VERSION = 0


class DatabaseException(Exception):
    pass


class Database:
    def __init__(self, path_list, verbose=False):
        self.dbs = []
        self.csv = []
        self.names = []
        self.verbose = verbose
        # init non-existent databases and check existing databases
        for path in path_list:
            if not os.path.isfile(path):
                self.init(path, VERSION, HASH_VERSION)
                self.dbs.append(path)
            elif self.is_database(path):
                self.check(path, VERSION, HASH_VERSION)
                self.dbs.append(path)
            else:
                self.csv.append(path)
        # connect to main database self.dbs[0] and attach all others
        if len(self.dbs) > 0:
            self.connection = sqlite3.connect(self.dbs[0], uri=True)
            self.cursor = self.connection.cursor()
            self.names.append(os.path.splitext(os.path.basename(self.dbs[0]))[0])
            for path in self.dbs[1:]:
                name = os.path.splitext(os.path.basename(path))[0]
                self.names.append(name)
                self.cursor.execute("ATTACH DATABASE '{}' AS {}".format(path, name))
        # import csv-files and attach in-memory database
        if len(self.csv) > 0:
            self.inmemory = sqlite3.connect("file::memory:?cache=shared", uri=True)
            for path in self.csv:
                self.import_csv(path, self.inmemory)
            if len(self.dbs) == 0:
                eprint("Warning: Read-only mode (only csv-files given)")
                self.connection = self.inmemory
                self.cursor = self.connection.cursor()
            else:
                self.cursor = self.connection.cursor()
                self.cursor.execute("ATTACH DATABASE 'file::memory:?cache=shared' AS _in_memory_")
            self.names.append("_in_memory_")
        
    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.connection.commit()
        self.connection.close()
        if len(self.csv) > 0:
            self.inmemory.close()

    def is_database(self, db):
        if not os.path.isfile(db): return False
        sz = os.path.getsize(db)

        # file is empty, give benefit of the doubt that its sqlite
        # New sqlite3 files created in recent libraries are empty!
        if sz == 0: return True

        # SQLite database file header is 100 bytes
        if sz < 100: return False
        
        # Validate file header
        with open(db, 'rb') as fd: header = fd.read(100)    

        return (header[:16] == b'SQLite format 3\x00')

    def import_csv(self, path, con):
        im_cursor = con.cursor()
        with open(path) as csvfile:
            csvreader = csv.DictReader(csvfile)
            for source in csvreader.fieldnames:
                if source != "hash":
                    im_cursor.execute('CREATE TABLE IF NOT EXISTS {} (hash TEXT NOT NULL, value TEXT NOT NULL)'.format(source))
                    lst = [(row["hash"].strip(), row[source].strip()) for row in csvreader if row[source] and row[source].strip()]
            im_cursor.executemany("INSERT INTO {} VALUES (?,?)".format(source), lst)
        con.commit()

    def init(self, path, version, hash_version):
        eprint("Intitializing Fresh Database {}".format(path))
        con = sqlite3.connect(path)
        cur = con.cursor()
        cur.execute("CREATE TABLE __version (entry UNIQUE, version INT, hash_version INT)")
        cur.execute("INSERT INTO __version (entry, version, hash_version) VALUES (0, {}, {})".format(version, hash_version))
        con.commit()
        con.close()        

    def check(self, path, version, hash_version):
        con = sqlite3.connect(path)
        cur = con.cursor()        
        tables = [x[0] for x in cur.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")]
        views = [x[0] for x in cur.execute("SELECT tbl_name FROM sqlite_master WHERE type='view'")]

        # version control
        if not "__version" in tables:
            eprint("WARNING: Version info not available in database {}".format(path))
            return

        __version = cur.execute("SELECT version, hash_version FROM __version").fetchall()
        if __version[0][0] != version:
            eprint("WARNING: DB Version is {} but tool version is {}".format(__version[0][0], version))
        if __version[0][1] != hash_version:
            eprint("WARNING: DB Hash-Version is {} but tool hash-version is {}".format(__version[0][1], hash_version))

        # meta data (one json-blob per feature)
        if not "__meta" in tables:
            cur.execute("CREATE TABLE IF NOT EXISTS __meta (name TEXT UNIQUE, value BLOB)")

        # create required features
        for context in config.contexts():
            if not prepend_context("local", context) in tables:
                cur.execute("CREATE TABLE {} (hash TEXT NOT NULL, value TEXT NOT NULL)".format(prepend_context("local", context)))
            if not prepend_context("filename", context) in views:
                cur.execute("CREATE VIEW IF NOT EXISTS {} (hash, value) AS SELECT hash, REPLACE(value, RTRIM(value, REPLACE(value, '/', '')), '') FROM {}".format(prepend_context("filename", context), prepend_context("local", context)))
            if not prepend_context("hash", context) in views:
                cur.execute("CREATE VIEW IF NOT EXISTS {} (hash, value) AS SELECT DISTINCT hash, hash FROM {}".format(prepend_context("hash", context), prepend_context("local", context)))

        # existence of context translators
        for (c0, c1) in list(itertools.combinations(config.contexts(), 2)):
            translator = "__translator_{}_{}".format(c0, c1)
            if not translator in tables:
                cur.execute("CREATE TABLE {} (hash TEXT NOT NULL, value TEXT NOT NULL)".format(translator))
            translator = "__translator_{}_{}".format(c1, c0)
            if not translator in tables:
                cur.execute("CREATE TABLE {} (hash TEXT NOT NULL, value TEXT NOT NULL)".format(translator))

        con.commit()
        con.close()

    def create_table(self, name, default_value=None):
        context = context_from_name(name)
        if default_value is not None:
            self.execute('CREATE TABLE IF NOT EXISTS {} (hash TEXT UNIQUE NOT NULL, value TEXT NOT NULL DEFAULT "{}")'.format(name, default_value))
            self.execute('INSERT OR IGNORE INTO {} (hash) SELECT hash FROM {}'.format(name, prepend_context("local", context)))
            self.execute('''CREATE TRIGGER {}_dval AFTER INSERT ON {} BEGIN 
                    INSERT OR IGNORE INTO {} (hash) VALUES (NEW.hash); END'''.format(name, prepend_context("local", context), name))
            # TODO: Delete Trigger; Sanitize Data by obsolete default values
        else:
            self.execute('CREATE TABLE IF NOT EXISTS {} (hash TEXT NOT NULL, value TEXT NOT NULL, CONSTRAINT all_unique UNIQUE(hash, value))'.format(name))

    def delete_table(self, name):
        self.execute('DROP TABLE IF EXISTS {}'.format(name))
        self.execute('DROP TRIGGER IF EXISTS {}_dval'.format(name))

    def rename_table(self, old_name, new_name):
        self.execute("ALTER TABLE {} RENAME TO {}".format(old_name, new_name))
        self.execute("UPDATE __meta SET name='{}' WHERE name='{}'".format(new_name, old_name))

    def query(self, q):
        if self.verbose:
            eprint(q)
        return self.cursor.execute(q).fetchall()

    def execute(self, q):
        if self.verbose:
            eprint(q)
        self.cursor.execute(q)

    def insert(self, feature, value, hashes, force=False):
        values = ', '.join(['("{}", "{}")'.format(hash, value) for hash in hashes])
        method = 'REPLACE' if force and self.table_unique(feature) else 'INSERT'
        self.execute('{} INTO {} (hash, value) VALUES {}'.format(method, feature, values))

    def delete_hashes(self, feature, hashes):
        dval = self.table_default_value(feature)
        if dval is None:
            self.execute("DELETE FROM {} WHERE hash IN ('{}')".format(feature, "', '".join(hashes)))
        else:
            self.insert(feature, dval, hashes, True)

    def delete_values(self, feature, values):        
        dval = self.table_default_value(feature)
        if dval is None:
            self.execute("DELETE FROM {} WHERE value IN ('{}')".format(feature, "', '".join(values)))
        else:
            self.execute("UPDATE TABLE {} SET value = {} WHERE value IN ('{}')".format(feature, dval, "', '".join(values)))
  
    def bulk_insert(self, table, lst):
        if self.table_unique(table):
            self.cursor.executemany("REPLACE INTO {} VALUES (?,?)".format(table), lst)
        else:
            self.cursor.executemany("INSERT INTO {} VALUES (?,?)".format(table), lst)

    def tables(self, tables=True, views=False, system=False):
        where = "NOT tbl_name LIKE 'sqlite\_%' escape '\\' AND type in ('table', 'view')"
        if not system:
            where = where + " AND NOT tbl_name LIKE '\_\_%' escape '\\'"
        if not views:
            where = where + " AND type != 'view'"
        if not tables:
            where = where + " AND type != 'table'"
        pat = r"SELECT tbl_name FROM {} WHERE {}"
        sql = [ pat.format("sqlite_master", where) ]
        for name in self.names[1:]:
            sql.append(pat.format(name + ".sqlite_master", where))
        lst = self.query(" UNION ".join(sql))
        return [x[0] for x in lst]

    def table_info(self, table):
        lst = self.query("PRAGMA table_info({})".format(table))
        columns = ('index', 'name', 'type', 'notnull', 'default_value', 'pk')
        table_infos = [dict(zip(columns, values)) for values in lst]
        return table_infos

    def index_list(self, table):
        lst = self.query("PRAGMA index_list({})".format(table))
        columns = ('seq', 'name', 'unique', 'origin', 'partial')
        index_list = [dict(zip(columns, values)) for values in lst]
        return index_list
    
    def index_info(self, index):
        tup = self.query("PRAGMA index_info({})".format(index))
        columns = ('index_rank', 'table_rank', 'name')
        index_info = dict(zip(columns, tup[0]))
        return index_info

    def table_info_augmented(self, table):
        table_infos = [info.update({'unique': False}) or info for info in self.table_info(table)]
        
        # determine unique columns
        index_list = self.index_list(table)
        for index in [e for e in index_list if e['unique']]:
            col = self.index_info(index['name'])['table_rank']
            table_infos[col]['unique'] = True

        for info in table_infos:
            if info['default_value'] is not None:
                info['default_value'] = info['default_value'].strip('"')
        
        return table_infos

    # return list of distinct values and value-range for numeric values
    def table_values(self, table):
        result = { "numeric" : [None, None], "discrete" : [] }
        records = self.query('SELECT DISTINCT value FROM {}'.format(table))
        for val in [record[0] for record in records]:
            if is_number(val):
                value = float(val)
                if result["numeric"][0] == None:
                    result["numeric"] = [value, value]
                elif value < result["numeric"][0]:
                    result["numeric"][0] = value
                elif value > result["numeric"][1]:
                    result["numeric"][1] = value
            else:
                result["discrete"].append(val)
        return result

    def table_size(self, table):
        return self.query('SELECT COUNT(*) FROM {}'.format(table))[0][0]

    def table_unique(self, table):
        return self.table_default_value(table) is not None

    def table_default_value(self, table):
        return self.table_info(table)[1]['default_value']

    def system_record(self, table_name):
        system_record = dict()
        system_record['table_name'] = table_name
        system_record['table_size'] = self.table_size(table_name)
        system_record['table_unique'] = self.table_unique(table_name)
        system_record['table_default'] = self.table_default_value(table_name)
        values = self.table_values(table_name)
        system_record['table_intmin'] = values['numeric'][0]
        system_record['table_intmax'] = values['numeric'][1]
        system_record['table_values'] = " ".join(values['discrete'])
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
