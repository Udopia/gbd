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
import os
import csv

from dataclasses import dataclass

import gbd_tool.config as config
from gbd_tool.gbd_hash import HASH_VERSION
from gbd_tool.util import eprint, prepend_context, context_from_name, make_alnum_ul


VERSION = 1


class SchemaException(Exception):
    pass


@dataclass
class FeatureInfo:
    name: str = None
    database: str = None
    context: str = None
    table: str = None
    column: str = None
    default: str = None
    virtual: bool = False


class Schema:
    IN_MEMORY_DB = "file::memory:?cache=shared"

    def __init__(self, path):
        self.contexts = []
        self.tables = []
        self.views = []
        self.features = []
        if self.is_database(path):
            self.dbname = make_alnum_ul(os.path.basename(path))
            self.path = path
            self.connection = sqlite3.connect(path)
            self.csv = False
            self.check_version(VERSION, HASH_VERSION)
            self.schema_from_database()
        else:
            self.dbname = "main"
            self.path = path
            self.connection = sqlite3.connect(self.IN_MEMORY_DB, uri=True)
            self.csv = True
            self.schema_from_csv()

    @classmethod
    def is_database(cls, path):
        if not os.path.isfile(path): 
            Schema.create(path, VERSION, HASH_VERSION)
            return True
        sz = os.path.getsize(path)
        if sz == 0: return True  # new sqlite3 files can be empty
        if sz < 100: return False  # sqlite header is 100 bytes
        with open(path, 'rb') as fd: header = fd.read(100)  # validate header
        return (header[:16] == b'SQLite format 3\x00')

    @classmethod
    def create(cls, path, version, hash_version):
        eprint("Warning: Creating Database {}".format(path))
        try:
            con = sqlite3.connect(path)
            con.cursor().execute("CREATE TABLE __version (version INT, hash_version INT)")
            con.cursor().execute("INSERT INTO __version (version, hash_version) VALUES ({}, {})".format(version, hash_version))
            con.commit()
            con.close()
        except Exception as e:
            raise SchemaException(str(e))

    def check_version(self, version, hash_version):
        try:
            __version = self.connection.execute("SELECT version, hash_version FROM __version").fetchall()
            if __version[0][0] != version:
                raise SchemaException("WARNING: Database schema version is {} but tool supported schema version is {}".format(__version[0][0], version))
            if __version[0][1] != hash_version:
                raise SchemaException("WARNING: Database hash version is {} but tool supported hash version is {}".format(__version[0][1], hash_version))
        except Exception as e:
            raise SchemaException(str(e))

    def absorb(self, schema):
        if not self.csv or not schema.csv:
            raise SchemaException("Internal Error: Attempt to merge non-virtual schemata")
        self.contexts.extend(schema.contexts)
        self.tables.extend(schema.tables)
        self.views.extend(schema.views)
        self.features.extend(schema.features)


    # Import CSV to IN_MEMORY_DB, create according schema info
    def schema_from_csv(self):
        filename = os.path.basename(self.path)
        table = make_alnum_ul(filename)
        context = context_from_name(filename)
        self.contexts.append(context)
        self.views.append(table)
        try:
            with open(self.path) as csvfile:
                csvreader = csv.DictReader(csvfile)
                if not "hash" in csvreader.fieldnames:
                    raise SchemaException("Column 'hash' not found in {}".format(csvfile))
                for title in csvreader.fieldnames:
                    self.features.append(FeatureInfo(prepend_context(title, context), self.dbname, context, table, title, None, True))
                self.connection.execute('CREATE TABLE IF NOT EXISTS {} ({})'.format(table, ", ".join(csvreader.fieldnames)))
                for row in csvreader:
                    self.connection.execute("INSERT INTO {} VALUES ('{}')".format(table, "', '".join(row.values())))
            self.connection.commit()
        except Exception as e:
            raise SchemaException(str(e))

    # Create schema info for sqlite database
    def schema_from_database(self):
        sql_tables="""SELECT tbl_name, type FROM sqlite_master WHERE type IN ('table', 'view') 
                        AND NOT tbl_name LIKE 'sqlite$_%' ESCAPE '$' AND NOT tbl_name LIKE '$_$_%' ESCAPE '$'"""
        tables = self.connection.execute(sql_tables).fetchall()
        for (table, type) in tables:
            context = context_from_name(table)
            if not context in self.contexts:
                self.contexts.append(context)
            if type == "view":
                self.views.append(table)
            else:
                self.tables.append(table)
            columns = self.connection.execute("PRAGMA table_info({})".format(table)).fetchall()
            names = ('index', 'name', 'type', 'notnull', 'default_value', 'pk')
            for record in columns:
                col = dict(zip(names, record))
                fname = table if col['name'] == "value" else col['name']
                if fname == "hash":
                    fname = prepend_context("hash", context)
                self.features.append(FeatureInfo(fname, self.dbname, context, table, col['name'], col['default_value'], type == "view"))
        for context in self.contexts:
            self.create_main_feature_table(context)


    def create_main_feature_table(self, context):
        self.valid_context_or_raise(context)
        main_table = prepend_context("features", context)
        if not main_table in self.tables:
            self.connection.execute("CREATE TABLE IF NOT EXISTS {} (hash UNIQUE NOT NULL)".format(main_table))
            for table in filter(lambda t: context == context_from_name(t), self.tables):
                self.connection.execute("INSERT OR IGNORE INTO {} (hash) SELECT DISTINCT(hash) FROM {}".format(main_table, table))
                self.connection.execute("""CREATE TRIGGER IF NOT EXISTS {}_dval AFTER INSERT ON {} 
                                            BEGIN INSERT OR IGNORE INTO {} (hash) VALUES (NEW.hash); END""".format(table, table, main_table))
                self.connection.commit()
            self.tables.append(main_table)
            if not context in self.contexts:
                self.contexts.append(context)
            self.features.append(FeatureInfo(prepend_context("hash", context), self.dbname, context, main_table, "hash", None, False))

    def create_context_translator_table(self, src, dst):
        self.valid_context_or_raise(src)
        self.valid_context_or_raise(dst)
        translator = "{}_to_{}".format(src, dst)
        if not translator in self.tables:
            self.connection.execute("CREATE TABLE IF NOT EXISTS {} (hash, value)".format(translator))
            self.connection.commit()
        self.tables.append(translator)
        if not src in self.contexts:
            self.contexts.append(src)
        self.features.append(FeatureInfo(prepend_context("hash", src), self.dbname, src, translator, "hash", None, False))


    @classmethod
    def is_main_hash_column(cls, info: FeatureInfo):
        is_main = prepend_context("features", info.context) == info.table
        is_hash = info.column == "hash"
        return is_main and is_hash

    @classmethod
    def valid_context_or_raise(cls, name):
        if not name in config.contexts():
            raise SchemaException("Unknown Context: " + name)

    @classmethod
    def valid_feature_or_raise(cls, name):
        if len(name) < 2:
            raise SchemaException("Feature name '{}' is to short.".format(name))
        gbd_keywords = [ 'hash', 'value', 'local', 'filename', 'features' ]
        if name.lower() in gbd_keywords or name.startswith("__"):
            raise SchemaException("Feature name '{}' is reserved.".format(name))
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
            raise SchemaException("Feature name '{}' is reserved by sqlite.".format(name))
