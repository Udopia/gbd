from gbd_tool.util import eprint, open_cnf_file
from gbd_tool.db import Database

from gbd_tool.gbd_hash import gbd_hash_sorted

import io
import os
import hashlib
import bz2
import tempfile

import multiprocessing
from multiprocessing import Pool, Lock

mutex = Lock()

def bootstrap(api, database, named_algo, jobs):
    if named_algo == 'clause_types':
        api.create_feature("clauses_horn", 0)
        api.create_feature("clauses_positive", 0)
        api.create_feature("clauses_negative", 0)
        api.create_feature("variables", 0)
        api.create_feature("clauses", 0)
        resultset = api.query_search("clauses = 0", ["local"])
        schedule_bootstrap(api, jobs, resultset, compute_clause_types)
    elif named_algo == 'degree_sequence_hash':
        api.create_feature("degree_sequence_hash", "empty")
        resultset = api.query_search("degree_sequence_hash = empty", ["local"])
        schedule_bootstrap(api, jobs, resultset, compute_degree_sequence_hash)
    else:
        raise NotImplementedError

def schedule_bootstrap(api, jobs, resultset, func):
    if jobs == 1:
        for result in resultset:
            hashvalue = result[0].split(',')[0]
            filename = result[1].split(',')[0]
            api.callback_set_attributes_locked(func(hashvalue, filename))
    else:
        pool = Pool(min(multiprocessing.cpu_count(), jobs))
        for result in resultset:
            hashvalue = result[0].split(',')[0]
            filename = result[1].split(',')[0]
            pool.apply_async(func, args=(hashvalue, filename), callback=api.callback_set_attributes_locked)
        pool.close()
        pool.join()

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
            if not len(clause):
                raise ValueError("clause is empty: {}".format(line))
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


def compute_degree_sequence_hash(hashvalue, filename):
    eprint('Computing sorted_hash for {}'.format(filename))
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