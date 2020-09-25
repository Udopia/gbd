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
import sqlite3

from multiprocessing import Pool, Lock
from os.path import isfile

from gbd_tool import search
from gbd_tool.db import Database
from gbd_tool.util import eprint, confirm

try:
    from gbdhashc import gbdhash as gbd_hash
except ImportError:
    from gbd_tool.gbd_hash import gbd_hash

mutex = Lock()

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
