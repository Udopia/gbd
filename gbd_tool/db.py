# Global Benchmark Database (GBD)
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

from gbd_tool.gbd_hash import HASH_VERSION
from gbd_tool.util import eprint, is_number

VERSION = 0


class DatabaseException(Exception):
    pass


class Database:

    def __init__(self, path_list):
        self.paths = path_list
        # init non-existent databases and check existing databases
        for path in self.paths:
            if not os.path.isfile(path):
                eprint("Initializing DB '{}' with version {} and hash-version {}".format(path, VERSION, HASH_VERSION))
                self.init(path, VERSION, HASH_VERSION)
            else:
                self.check(path, VERSION, HASH_VERSION)

    def __enter__(self):
        #eprint("Main connection: {}".format(self.paths[0]))
        self.connection = sqlite3.connect(self.paths[0])
        self.cursor = self.connection.cursor()
        for path in self.paths[1:]:
            name = os.path.splitext(os.path.basename(path))[0]
            #eprint("Attaching '{}' as {}".format(path, name))
            self.cursor.execute("ATTACH DATABASE '{}' AS {}".format(path, name))
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.commit()
        self.connection.close()

    def init(self, path, version, hash_version):
        con = sqlite3.connect(path)
        cur = con.cursor()
        cur.execute("CREATE TABLE __version (entry UNIQUE, version INT, hash_version INT)")
        cur.execute("INSERT INTO __version (entry, version, hash_version) VALUES (0, {}, {})".format(version, hash_version))
        cur.execute("CREATE TABLE __meta (name TEXT UNIQUE, value BLOB)")
        cur.execute("CREATE TABLE local (hash TEXT NOT NULL, value TEXT NOT NULL)")
        cur.execute("CREATE VIEW IF NOT EXISTS filename (hash, value) AS SELECT hash, REPLACE(value, RTRIM(value, REPLACE(value, '/', '')), '') FROM local")
        con.commit()
        con.close()

    def check(self, path, version, hash_version):
        con = sqlite3.connect(path)
        cur = con.cursor()
        lst = cur.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [x[0] for x in lst]
        if not "__version" in tables:
            eprint("WARNING: Version info not available in database {}".format(path))
            return
        __version = cur.execute("SELECT version, hash_version FROM __version").fetchall()
        if __version[0][0] != version:
            eprint("WARNING: DB Version is {} but tool version is {}".format(__version[0][0], version))
        if __version[0][1] != hash_version:
            eprint("WARNING: DB Hash-Version is {} but tool hash-version is {}".format(__version[0][1], hash_version))

        # upgrade legacy data-model
        if "filename" in tables:
            cur.execute("DROP TABLE IF EXISTS filename")        
            cur.execute("CREATE VIEW IF NOT EXISTS filename (hash, value) AS SELECT hash, REPLACE(value, RTRIM(value, REPLACE(value, '/', '')), '') FROM local")
            con.commit()

        if not "__meta" in tables:
            cur.execute("CREATE TABLE __meta (name TEXT UNIQUE, value BLOB)")

        con.close()

    def value_query(self, q):
        lst = self.cursor.execute(q).fetchall()
        return set([row[0] for row in lst])

    def query(self, q):
        return self.cursor.execute(q).fetchall()

    def submit(self, q):
        eprint(q)
        self.cursor.execute(q)
        self.commit()

    def bulk_insert(self, table, lst):
        if self.table_unique(table):
            self.cursor.executemany("REPLACE INTO {} VALUES (?,?)".format(table), lst)
        else:
            self.cursor.executemany("INSERT INTO {} VALUES (?,?)".format(table), lst)

    def commit(self):
        self.connection.commit()

    def tables_and_views(self):
        lst = self.query(r"SELECT tbl_name FROM sqlite_master WHERE (type='table' OR type='view') AND NOT tbl_name LIKE '\_\_%' escape '\' AND NOT tbl_name LIKE 'sqlite\_%' escape '\'")
        return [x[0] for x in lst]

    def tables(self):
        lst = self.query(r"SELECT tbl_name FROM sqlite_master WHERE type='table' AND NOT tbl_name LIKE '\_\_%' escape '\' AND NOT tbl_name LIKE 'sqlite\_%' escape '\'")
        return [x[0] for x in lst]

    def views(self):
        lst = self.query(r"SELECT tbl_name FROM sqlite_master WHERE type='view' AND NOT tbl_name LIKE '\_\_%' escape '\' AND NOT tbl_name LIKE 'sqlite\_%' escape '\'")
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
        values = self.value_query('SELECT DISTINCT value FROM {}'.format(table))
        for value in values:
            if is_number(value):
                if result["numeric"][0] == None:
                    result["numeric"] = [value, value]
                elif value < result["numeric"][0]:
                    result["numeric"][0] = value
                elif value > result["numeric"][1]:
                    result["numeric"][1] = value
            else:
                result["discrete"].append(value)
        return result

    def table_size(self, table):
        return self.value_query('SELECT COUNT(*) FROM {}'.format(table)).pop()

    def table_unique(self, table):
        return self.table_info_augmented(table)[0]['unique']

    def table_default_value(self, table):
        return self.table_info_augmented(table)[1]['default_value']

    def meta_get(self, table):
        return json.loads((self.value_query("SELECT value FROM __meta WHERE name = '{}'".format(table)) or {"{}"}).pop())

    def meta_clear(self, table, meta_feature=None):
        if not meta_feature:
            self.submit("INSERT OR REPLACE INTO __meta (name, value) VALUES ('{}', '')".format(table))
        else:
            values = self.meta_get(table)
            if meta_feature in values:
                values.pop(meta_feature)
            self.submit("INSERT OR REPLACE INTO __meta (name, value) VALUES ('{}', '{}')".format(table, json.dumps(values)))

    def meta_set(self, table, meta_feature, value):
        values = self.meta_get(table)
        values[meta_feature] = value
        self.submit("INSERT OR REPLACE INTO __meta (name, value) VALUES ('{}', '{}')".format(table, json.dumps(values)))


