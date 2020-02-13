# Global Benchmark Database (GBD)
# Copyright (C) 2019 Markus Iser, Luca Springer, Karlsruhe Institute of Technology (KIT)
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
from multiprocessing import Pool, Lock
from os.path import isfile

from gbd_tool.database import groups
from gbd_tool.database import search
from gbd_tool.database.db import Database
from gbd_tool.hashing.gbd_hash import gbd_hash
from gbd_tool.util import eprint

mutex = Lock()


def add_tag(database, cat, tag, hash, force=False):
    info = groups.reflect(database, cat)
    if (info[0]['unique']):
        if force:
            database.submit('REPLACE INTO {} (hash, value) VALUES ("{}", "{}")'.format(cat, hash, tag))
        else:
            res = database.query("SELECT value FROM {} WHERE hash='{}'".format(cat, hash))
            if (len(res) == 0):
                database.submit('INSERT INTO {} (hash, value) VALUES ("{}", "{}")'.format(cat, hash, tag))
            else:
                value = res[0][0]
                if value == info[1]['default_value']:
                    eprint("Overwriting default-value {} with new value {} for hash {}".format(value, tag, hash))
                    database.submit('REPLACE INTO {} (hash, value) VALUES ("{}", "{}")'.format(cat, hash, tag))
                elif value != tag:
                    eprint(
                        "Unable to insert tag ({}, {}) into unique '{}' as a different value is already set: '{}'".format(
                            hash, tag, cat, value))
    else:
        res = database.value_query("SELECT hash FROM {} WHERE hash='{}' AND value='{}'".format(cat, hash, tag))
        if (len(res) == 0):
            database.submit('INSERT INTO {} (hash, value) VALUES ("{}", "{}")'.format(cat, hash, tag))


def remove_tag(database, cat, tag, hash):
    database.submit("DELETE FROM {} WHERE hash='{}' AND value='{}'".format(cat, hash, tag))


def add_benchmark(database, hash, path):
    database.submit('INSERT INTO benchmarks (hash, value) VALUES ("{}", "{}")'.format(hash, path))
    g = groups.reflect(database)
    for group in g:
        info = groups.reflect(database, group)
        dval = info[1]['default_value']
        if (dval is not None):
            database.submit('INSERT OR IGNORE INTO {} (hash) VALUES ("{}")'.format(group, hash))


def remove_benchmarks(database):
    paths = database.value_query("SELECT value FROM benchmarks")
    for p in paths:
        if not isfile(p):
            eprint("Problem '{}' not found. Removing...".format(p))
            database.submit("DELETE FROM benchmarks WHERE value='{}'".format(p))


def safe_hash_locked(arg):
    mutex.acquire()
    try:
        # create new connection from old one due to limitations of multi-threaded use (cursor initialization issue)
        with Database(arg['database_path']) as database:
            add_benchmark(database, arg['hash_new'], arg['path'])
    finally:
        mutex.release()

def compute_hash(database_path, path):
    eprint('Hashing {}'.format(path))
    hash_new = gbd_hash(path)
    return { 'database_path': database_path, 'path': path, 'hash_new': hash_new }

def register_benchmarks(database, root):
    eprint('Hashing CNF files in {}'.format(root))
    pool = Pool(multiprocessing.cpu_count())
    for root, dirnames, filenames in os.walk(root):
        for filename in filenames:
            path = os.path.join(root, filename)
            if path.endswith(".cnf") or path.endswith(".cnf.gz") or path.endswith(".cnf.lzma") or path.endswith(".cnf.bz2"):
                hashes = database.value_query("SELECT hash FROM benchmarks WHERE value = '{}'".format(path))
                if len(hashes) is not 0:
                    eprint('Problem {} already hashed'.format(path))
                else:
                    eprint('Hash in pool {}'.format(filename))
                    handler = pool.apply_async(compute_hash, args=(database.path, path), callback=safe_hash_locked)
                    handler.get()
    pool.close()
    pool.join() 


def update_hash_locked(arg):
    mutex.acquire()
    try:
        # create new connection from old one due to limitations of multi-threaded use (cursor initialization issue)
        with Database(arg['database_path']) as database:
            tables = groups.reflect(database)
            for table in tables:
                if not table.startswith('__'):
                    database.submit("UPDATE {} SET hash={} WHERE hash={}".format(arg['table'], arg['hash_new'], arg['hash_old']))
    finally:
        mutex.release()

def compute_hash_for_update(database_path, path, hash_old):
    eprint('Computing gbd-hash for {}'.format(path))
    hash_new = gbd_hash(path)
    return { 'database_path': database_path, 'hash_old': hash_old, 'hash_new': hash_new }

def rehash_benchmarks(database):
    pool = Pool(multiprocessing.cpu_count())
    resultset = search.find_hashes(database, "", "benchmarks")
    for result in resultset:
        hash_old = result[0]
        filename = result[1].split(',')[0]
        eprint("Updating gbd-hash {} for file {}".format(hash_old, filename))
        handler = pool.apply_async(compute_hash_for_update, args=(database.path, filename, hash_old), callback=update_hash_locked)
        handler.get()
    pool.close()
    pool.join() 
