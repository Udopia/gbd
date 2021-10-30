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
    def __init__(self, path_list, verbose=False, context='cnf'):
        self.paths = []
        self.csv = []
        self.verbose = verbose
        self.context = context
        # init non-existent databases and check existing databases
        for path in path_list:
            if not os.path.isfile(path):
                eprint("{}: file does not exist, creating...".format(path))
                self.init(path, VERSION, HASH_VERSION)
                self.paths.append(path)
            else:
                try:
                    self.check(path, VERSION, HASH_VERSION)
                    self.paths.append(path)
                except sqlite3.DatabaseError as err:
                    eprint(str(err))
                    break
                    with open(path, 'r') as obj:
                        csvreader = csv.DictReader(obj)
                        if not "hash" in csvreader.fieldnames:
                            eprint("{}: file is neither a database nor csv-file having the key-column 'hash'".format(path))
                        else:
                            self.csv.append(path)
        
    def __enter__(self):
        self.inmemory = sqlite3.connect("file::memory:?cache=shared", uri=True)
        for path in self.csv:
            self.import_csv(path, self.inmemory)
        if len(self.paths) > 0:
            #eprint("Main connection: {}".format(self.paths[0]))
            self.connection = sqlite3.connect(self.paths[0], uri=True)
            self.cursor = self.connection.cursor()
            self.names = [ os.path.splitext(os.path.basename(self.paths[0]))[0] ]
            for path in self.paths[1:]:
                name = os.path.splitext(os.path.basename(path))[0]
                self.names.append(name)
                #eprint("Attaching '{}' as {}".format(path, name))
                self.cursor.execute("ATTACH DATABASE '{}' AS {}".format(path, name))
            if len(self.csv) > 0:
                self.cursor.execute("ATTACH DATABASE 'file::memory:?cache=shared' AS _in_memory_")
        else:
            self.connection = sqlite3.connect("file::memory:?cache=shared", uri=True)
            self.cursor = self.connection.cursor()
            self.names = [ "_in_memory_" ]
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.commit()
        self.connection.close()
        self.inmemory.close()

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
        con = sqlite3.connect(path)
        cur = con.cursor()
        cur.execute("CREATE TABLE __version (entry UNIQUE, version INT, hash_version INT)")
        cur.execute("INSERT INTO __version (entry, version, hash_version) VALUES (0, {}, {})".format(version, hash_version))
        cur.execute("CREATE TABLE __meta (name TEXT UNIQUE, value BLOB)")
        self.create_local(cur)
        self.create_filename(cur)
        self.create_hash(cur)
        con.commit()
        con.close()

    def create_local(self, cur):
        cur.execute("CREATE TABLE {} (hash TEXT NOT NULL, value TEXT NOT NULL)".format(prepend_context("local", self.context)))

    def create_filename(self, cur):
        cur.execute("CREATE VIEW IF NOT EXISTS {} (hash, value) AS SELECT hash, REPLACE(value, RTRIM(value, REPLACE(value, '/', '')), '') FROM {}".format(prepend_context("filename", self.context), prepend_context("local", self.context)))

    def create_hash(self, cur):
        cur.execute("CREATE VIEW IF NOT EXISTS {} (hash, value) AS SELECT DISTINCT hash, hash FROM {}".format(prepend_context("hash", self.context), prepend_context("local", self.context)))

    def check(self, path, version, hash_version):
        con = sqlite3.connect(path)
        cur = con.cursor()        
        tables = [x[0] for x in cur.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")]

        # version control
        if not "__version" in tables:
            eprint("WARNING: Version info not available in database {}".format(path))
            return

        __version = cur.execute("SELECT version, hash_version FROM __version").fetchall()
        if __version[0][0] != version:
            eprint("WARNING: DB Version is {} but tool version is {}".format(__version[0][0], version))
        if __version[0][1] != hash_version:
            eprint("WARNING: DB Hash-Version is {} but tool hash-version is {}".format(__version[0][1], hash_version))

        # existence of context translators
        for (c0, c1) in list(itertools.combinations(config.contexts(), 2)):
            translator = "__translator_{}_{}".format(c0, c1)
            if not translator in tables:
                cur.execute("CREATE TABLE {} (hash TEXT NOT NULL, value TEXT NOT NULL)".format(translator))
            translator = "__translator_{}_{}".format(c1, c0)
            if not translator in tables:
                cur.execute("CREATE TABLE {} (hash TEXT NOT NULL, value TEXT NOT NULL)".format(translator))

        # meta data (one json-blob per feature)
        if not "__meta" in tables:
            cur.execute("CREATE TABLE IF NOT EXISTS __meta (name TEXT UNIQUE, value BLOB)")

        # create required features
        if not prepend_context("local", self.context) in tables:
            self.create_local(cur)
        if not prepend_context("filename", self.context) in tables:
            self.create_filename(cur)
        if not prepend_context("hash", self.context) in tables:
            self.create_hash(cur)

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
        self.commit()

    def delete_table(self, name):
        self.execute('DROP TABLE IF EXISTS {}'.format(name))
        self.execute('DROP TRIGGER IF EXISTS {}_dval'.format(name))
        self.commit()

    def rename_table(self, old_name, new_name):
        self.execute("ALTER TABLE {} RENAME TO {}".format(old_name, new_name))
        self.execute("UPDATE __meta SET name='{}' WHERE name='{}'".format(new_name, old_name))
        self.commit()

    def query(self, q):
        if self.verbose:
            eprint(q)
        return self.cursor.execute(q).fetchall()

    def submit(self, q):
        self.execute(q)
        self.commit()

    def execute(self, q):
        if self.verbose:
            eprint(q)
        self.cursor.execute(q)

    def commit(self):
        self.connection.commit()

    def bulk_insert(self, table, lst):
        if self.table_unique(table):
            self.cursor.executemany("REPLACE INTO {} VALUES (?,?)".format(table), lst)
        else:
            self.cursor.executemany("INSERT INTO {} VALUES (?,?)".format(table), lst)

    def tables_and_views(self):
        pat = r"SELECT tbl_name FROM {} WHERE (type='table' OR type='view') AND NOT tbl_name LIKE '\_\_%' escape '\' AND NOT tbl_name LIKE 'sqlite\_%' escape '\'"
        sql = [ pat.format("sqlite_master") ]
        for name in self.names[1:]:
            sql.append(pat.format(name + ".sqlite_master"))
        lst = self.query(" UNION ".join(sql))
        return [x[0] for x in lst]

    def tables(self):
        pat = r"SELECT tbl_name FROM {} WHERE type='table' AND NOT tbl_name LIKE '\_\_%' escape '\' AND NOT tbl_name LIKE 'sqlite\_%' escape '\'"
        sql = [ pat.format("sqlite_master") ]
        for name in self.names[1:]:
            sql.append(pat.format(name + ".sqlite_master"))
        lst = self.query(" UNION ".join(sql))
        return [x[0] for x in lst]

    def views(self):
        pat = r"SELECT tbl_name FROM {} WHERE type='view' AND NOT tbl_name LIKE '\_\_%' escape '\' AND NOT tbl_name LIKE 'sqlite\_%' escape '\'"
        sql = [ pat.format("sqlite_master") ]
        for name in self.names[1:]:
            sql.append(pat.format(name + ".sqlite_master"))
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
        return self.table_info_augmented(table)[1]['default_value']

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
            self.submit("REPLACE INTO __meta (name, value) VALUES ('{}', '')".format(table))
        else:
            values = self.meta_record(table)
            if meta_feature in values:
                values.pop(meta_feature)
            self.submit("REPLACE INTO __meta (name, value) VALUES ('{}', '{}')".format(table, json.dumps(values)))

    def meta_set(self, table, meta_feature, value):
        values = self.meta_record(table)
        values[meta_feature] = value
        self.submit("REPLACE INTO __meta (name, value) VALUES ('{}', '{}')".format(table, json.dumps(values)))
