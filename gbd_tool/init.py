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

import multiprocessing
from multiprocessing import Pool

import os
from os.path import isfile

import hashlib
import csv

from gbd_tool.gbd_api import GBD, GBDException
from gbd_tool.gbd_hash import gbd_hash
from gbd_tool.util import eprint, confirm, open_cnf_file


# Import data from CSV file
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
    register_benchmarks(api, path)

def remove_stale_benchmarks(api: GBD):
    eprint("Sanitizing local path entries ... ")
    paths = api.database.value_query("SELECT value FROM local")
    sanitize = list(filter(lambda path: not isfile(path), paths))
    if len(sanitize) and confirm("{} files not found. Remove stale entries from local table?".format(len(sanitize))):
        for path in sanitize:
            api.database.submit("DELETE FROM local WHERE value='{}'".format(path))

def compute_hash(path):
    eprint('Hashing {}'.format(path))
    hashvalue = gbd_hash(path)
    attributes = [ ('INSERT', 'local', path) ]
    return { 'hashvalue': hashvalue, 'attributes': attributes }

def register_benchmarks(api: GBD, root):
    pool = Pool(min(multiprocessing.cpu_count(), api.jobs))
    for root, dirnames, filenames in os.walk(root):
        for filename in filenames:
            path = os.path.join(root, filename)
            if any(path.endswith(suffix) for suffix in [".cnf", ".cnf.gz", ".cnf.lzma", ".cnf.xz", ".cnf.bz2"]):
                hashes = api.database.value_query("SELECT hash FROM local WHERE value = '{}'".format(path))
                if len(hashes) != 0:
                    eprint('Problem {} already hashed'.format(path))
                else:
                    handler = pool.apply_async(compute_hash, args=(path,), callback=api.callback_set_attributes_locked)
                    #handler.get()
    pool.close()
    pool.join() 


# Generic Parallel Runner
def run(api: GBD, resultset, func):
    if api.jobs == 1:
        for result in resultset:
            hashvalue = result[0].split(',')[0]
            filename = result[1].split(',')[0]
            api.callback_set_attributes_locked(func(hashvalue, filename))
    else:
        pool = Pool(min(multiprocessing.cpu_count(), api.jobs))
        for result in resultset:
            hashvalue = result[0].split(',')[0]
            filename = result[1].split(',')[0]
            pool.apply_async(func, args=(hashvalue, filename), callback=api.callback_set_attributes_locked)
        pool.close()
        pool.join()


# Initialize clause-type tables for given instances
def init_clause_types(api: GBD, hashes):
    for table in [ "clauses_horn", "clauses_positive", "clauses_negative", "variables", "clauses" ]:
        if not api.feature_exists(table):
            api.create_feature(table, "empty")
    resultset = api.query_search(None, hashes, ["local"])
    run(api, resultset, compute_clause_types)

def compute_clause_types(hashvalue, filename):
    eprint('Computing clause_types for {}'.format(filename))
    c_vars = 0
    c_clauses = 0
    c_horn = 0
    c_pos = 0
    c_neg = 0
    f = open_cnf_file(filename, 'rt')
    for line in f:
        line = line.strip()
        if line and line[0] not in ['p', 'c']:
            clause = [int(lit) for lit in line.split()[:-1]]
            c_vars = max(c_vars, max(abs(lit) for lit in clause))
            c_clauses += 1
            n_pos = sum(lit > 0 for lit in clause)
            if n_pos < 2:
                c_horn += 1
                if n_pos == 0:
                    c_neg += 1
            if n_pos == len(clause):
                c_pos += 1
    f.close()
    attributes = [ ('REPLACE', 'clauses_horn', c_horn), ('REPLACE', 'clauses_positive', c_pos), ('REPLACE', 'clauses_negative', c_neg), 
                   ('REPLACE', 'variables', c_vars), ('REPLACE', 'clauses', c_clauses) ]
    return { 'hashvalue': hashvalue, 'attributes': attributes }


# Initialize degree_sequence_hash for given instances
def init_degree_sequence_hash(api: GBD, hashes):
    if not api.feature_exists("degree_sequence_hash"):
        api.create_feature("degree_sequence_hash", "empty")
    resultset = api.query_search(None, hashes, ["local"])
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