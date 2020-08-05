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

import multiprocessing
import os
import csv

from multiprocessing import Pool, Lock
from os.path import isfile

from gbd_tool import groups
from gbd_tool import search
from gbd_tool.db import Database
from gbd_tool.gbd_hash import gbd_hash
from gbd_tool.util import eprint, confirm

mutex = Lock()

def import_csv(database, filename, key, source, target, delim_=' '):
    if target not in database.tables():
        print("Target group {} does not exist. Import canceled.".format(target))
    with open(filename, newline='') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter=delim_, quotechar='\'')
        lst = [(row[key].strip(), row[source].strip()) for row in csvreader if row[source] and row[source].strip()]
        print("Inserting {} values into group {}".format(len(lst), target))
        database.bulk_insert(target, lst)

def add_tag(database, name, value, hash, force=False):
    if database.table_unique(name):
        if force:
            database.submit('REPLACE INTO {} (hash, value) VALUES ("{}", "{}")'.format(name, hash, value))
        else:
            res = database.query("SELECT value FROM {} WHERE hash='{}'".format(name, hash))
            if (len(res) == 0):
                database.submit('INSERT INTO {} (hash, value) VALUES ("{}", "{}")'.format(name, hash, value))
            else:
                existing_value = res[0][0]
                default_value = database.table_default_value(name)
                if existing_value == default_value:
                    eprint("Overwriting default-value {} with new value {} for hash {}".format(default_value, value, hash))
                    database.submit('REPLACE INTO {} (hash, value) VALUES ("{}", "{}")'.format(name, hash, value))
                elif existing_value != value:
                    eprint("Unable to insert tag ({}, {}) into unique '{} (default: {})' as a different value is already set: '{}'".format(hash, value, name, default_value, existing_value))
    else:
        res = database.value_query("SELECT hash FROM {} WHERE hash='{}' AND value='{}'".format(name, hash, value))
        if (len(res) == 0):
            database.submit('INSERT INTO {} (hash, value) VALUES ("{}", "{}")'.format(name, hash, value))


def remove_tags(database, name, hash_list):
    database.submit("DELETE FROM {} WHERE hash IN ('{}')".format(name, "', '".join(hash_list)))

def remove_benchmarks(database):
    eprint("Sanitizing local path entries ... ")
    paths = database.value_query("SELECT value FROM local")
    sanitize = list(filter(lambda path: not isfile(path), paths))
    if len(sanitize) and confirm("{} files not found, remove local path entries from database?".format(len(sanitize))):
        for path in sanitize:
            eprint("File '{}' not found, removing path entry.".format(path))
            database.submit("DELETE FROM local WHERE value='{}'".format(path))

def compute_hash(path):
    eprint('Hashing {}'.format(path))
    hashvalue = gbd_hash(path)
    attributes = [ ('INSERT', 'local', path) ]
    return { 'hashvalue': hashvalue, 'attributes': attributes }

def register_benchmarks(api, database, root, jobs=1):
    pool = Pool(min(multiprocessing.cpu_count(), jobs))
    for root, dirnames, filenames in os.walk(root):
        for filename in filenames:
            path = os.path.join(root, filename)
            if path.endswith(".cnf") or path.endswith(".cnf.gz") or path.endswith(".cnf.lzma") or path.endswith(".cnf.xz") or path.endswith(".cnf.bz2"):
                hashes = database.value_query("SELECT hash FROM local WHERE value = '{}'".format(path))
                if len(hashes) != 0:
                    eprint('Problem {} already hashed'.format(path))
                else:
                    handler = pool.apply_async(compute_hash, args=(path,), callback=api.callback_set_attributes_locked)
                    #handler.get()
    pool.close()
    pool.join() 
