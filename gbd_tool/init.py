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

from concurrent.futures.process import BrokenProcessPool
import multiprocessing
from multiprocessing import Pool

import os
from os.path import isfile

import psutil
import time
import hashlib
import csv

from concurrent.futures import ProcessPoolExecutor, wait, FIRST_EXCEPTION, as_completed, CancelledError
from asyncio import CancelledError

from gbd_tool.gbd_api import GBD, GBDException
from gbd_tool.gbd_hash import gbd_hash
from gbd_tool.util import eprint, confirm, open_cnf_file

#import faulthandler
#faulthandler.enable()

try:
    from gbdc import extract_base_features
except ImportError:
    def extract_base_features(path) -> dict:
        raise GBDException("Method 'extract_base_features' not available. Install cnftools' GDBC Accelerator Module.")

try:
    from gbdc import extract_gate_features
except ImportError:
    def extract_gate_features(path) -> dict:
        raise GBDException("Method 'extract_gate_features' not available. Install cnftools' GDBC Accelerator Module.")


def import_csv(api: GBD, path, key, source, target):
    if not api.feature_exists(target):
        raise GBDException("Target feature '{}' does not exist. Import canceled.".format(target))
    with open(path, newline='') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter=api.separator, quotechar='\'')
        lst = [(row[key].strip(), row[source].strip()) for row in csvreader if row[source] and row[source].strip()]
        eprint("Inserting {} values into target '{}'".format(len(lst), target))
        api.database.bulk_insert(target, lst)


# Initialize table 'local' with instances found under given path
def init_local(api: GBD, path):
    eprint('Initializing local path entries {} using {} cores'.format(path, api.jobs))
    if api.jobs == 1 and multiprocessing.cpu_count() > 1:
        eprint("Activate parallel initialization using --jobs={}".format(multiprocessing.cpu_count()))
    remove_stale_benchmarks(api)
    init_benchmarks(api, path)

def remove_stale_benchmarks(api: GBD):
    eprint("Sanitizing local path entries ... ")
    paths = api.database.value_query("SELECT value FROM local")
    sanitize = list(filter(lambda path: not isfile(path), paths))
    if len(sanitize) and confirm("{} files not found. Remove stale entries from local table?".format(len(sanitize))):
        for path in sanitize:
            api.database.submit("DELETE FROM local WHERE value='{}'".format(path))

def compute_hash(nohashvalue, path):
    eprint('Hashing {}'.format(path))
    hashvalue = gbd_hash(path)
    attributes = [ ('INSERT', 'local', path) ]
    return { 'hashvalue': hashvalue, 'attributes': attributes }

def init_benchmarks(api: GBD, root):
    resultset = []
    for root, dirnames, filenames in os.walk(root):
        for filename in filenames:
            path = os.path.join(root, filename)
            if any(path.endswith(suffix) for suffix in [".cnf", ".cnf.gz", ".cnf.lzma", ".cnf.xz", ".cnf.bz2"]):
                hashes = api.database.value_query("SELECT hash FROM local WHERE value = '{}'".format(path))
                if len(hashes) != 0:
                    eprint('Problem {} already hashed'.format(path))
                else:
                    resultset.append(("", path))
    run(api, resultset, compute_hash)


# Parallel Runner
def run(api: GBD, resultset, func):
    if api.jobs == 1:
        for result in resultset:
            api.callback_set_attributes_locked(func(result[0], result[1]))  # hash, local
    else:
        while len(resultset) > 0:
            eprint("Starting ProcessPoolExecutor with {} jobs".format(len(resultset)))
            with ProcessPoolExecutor(min(multiprocessing.cpu_count(), api.jobs)) as p:
                futures = {
                    p.submit(func, hash, local): (hash, local)
                    for (hash, local) in resultset[:max(multiprocessing.cpu_count(), api.jobs)]
                }
                try:
                    for f in as_completed(futures, timeout=api.timeout):
                        e = f.exception()
                        if e is not None:
                            if type(e) == BrokenProcessPool:
                                try:
                                    eprint("{}: {} in {}".format(e.__class__.__name__, e, futures[f]))
                                    resultset.remove(futures[f])
                                    break
                                except ValueError as err:
                                    eprint("Value error: {} {}".format(err, f))
                        else:
                            resultset.remove(futures[f])
                            api.callback_set_attributes_locked(f.result())
                except futures.TimeoutError as e:
                    eprint("{}: {}".format(e.__class__.__name__, e))


# Initialize base feature tables for given instances
def init_base_features(api: GBD, query, hashes):
    resultset = api.query_search(query, hashes, ["local"], collapse="MIN")
    run(api, resultset, base_features)

def base_features(hashvalue, filename):
    mem = psutil.virtual_memory()
    if mem.available < 1024 * 1024 * 1024:
        raise CancelledError
    eprint('Extracting base features from {}'.format(filename))
    rec = extract_base_features(filename)
    eprint('Done with base features from {}'.format(filename))
    attributes = [ ('REPLACE', key, int(value) if value.is_integer() else value) for key, value in rec.items() ]
    return { 'hashvalue': hashvalue, 'attributes': attributes }


# Initialize gate feature tables for given instances
def init_gate_features(api: GBD, query, hashes):
    resultset = api.query_search(query, hashes, ["local"], collapse="MIN")
    run(api, resultset, gate_features)

def gate_features(hashvalue, filename):
    mem = psutil.virtual_memory()
    if mem.available < 1024 * 1024 * 1024:
        raise CancelledError
    eprint('Extracting gate features from {}'.format(filename))
    rec = extract_gate_features(filename)
    eprint('Done with gate features from {}'.format(filename))
    attributes = [ ('REPLACE', key, int(value) if value.is_integer() else value) for key, value in rec.items() ]
    return { 'hashvalue': hashvalue, 'attributes': attributes }


# Initialize degree_sequence_hash for given instances
def init_degree_sequence_hash(api: GBD, hashes):
    if not api.feature_exists("degree_sequence_hash"):
        api.create_feature("degree_sequence_hash", "empty")
    resultset = api.query_search(None, hashes, ["local"], collapse="MIN")
    run(api, resultset, compute_degree_sequence_hash)

def compute_degree_sequence_hash(hashvalue, filename):
    eprint('Computing degree-sequence hash for {}'.format(filename))
    hash_md5 = hashlib.md5()
    degrees = dict()
    f = open_cnf_file(filename, 'rt')
    for line in f:
        line = line.strip()
        if line and line[0] not in ['p', 'c']:
            for lit in line.split()[:-1]:
                num = int(lit)
                tup = degrees.get(abs(num), (0,0))
                degrees[abs(num)] = (tup[0], tup[1]+1) if num < 0 else (tup[0]+1, tup[1])

    degree_list = list(degrees.values())
    degree_list.sort(key=lambda t: (t[0]+t[1], abs(t[0]-t[1])))
    
    for t in degree_list:
        hash_md5.update(str(t[0]+t[1]).encode('utf-8'))
        hash_md5.update(b' ')
        hash_md5.update(str(abs(t[0]-t[1])).encode('utf-8'))
        hash_md5.update(b' ')

    f.close()

    return { 'hashvalue': hashvalue, 'attributes': [ ('REPLACE', 'degree_sequence_hash', hash_md5.hexdigest()) ] }