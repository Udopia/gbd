from gbd_tool.util import eprint, open_cnf_file
from gbd_tool.db import Database

from gbd_tool.gbd_hash import gbd_hash_sorted

import io

import multiprocessing
from multiprocessing import Pool, Lock

mutex = Lock()


def bootstrap(api, database, named_algo, jobs):
    if named_algo == 'clause_types':
        api.add_attribute_group("clauses_horn", "integer", 0)
        api.add_attribute_group("clauses_positive", "integer", 0)
        api.add_attribute_group("clauses_negative", "integer", 0)
        api.add_attribute_group("variables", "integer", 0)
        api.add_attribute_group("clauses", "integer", 0)
        resultset = api.query_search("clauses = 0", ["local"])
        schedule_bootstrap(database.path, jobs, resultset, compute_clause_types)
    elif named_algo == 'sorted_hash':
        api.add_attribute_group("sorted_hash", "text", "empty")
        resultset = api.query_search("sorted_hash = empty", ["local"])
        schedule_bootstrap(database.path, jobs, resultset, compute_sorted_hash)
    else:
        raise NotImplementedError

def schedule_bootstrap(database_path, jobs, resultset, func):
    if jobs == 1:
        for result in resultset:
            hashvalue = result[0].split(',')[0]
            filename = result[1].split(',')[0]
            safe_locked(func(database_path, hashvalue, filename))
    else:
        pool = Pool(min(multiprocessing.cpu_count(), jobs))
        for result in resultset:
            hashvalue = result[0].split(',')[0]
            filename = result[1].split(',')[0]
            pool.apply_async(func, args=(database_path, hashvalue, filename), callback=safe_locked)
        pool.close()
        pool.join() 

def safe_locked(arg):
    mutex.acquire()
    try:
        # create new connection from old one due to limitations of multi-threaded use (cursor initialization issue)
        with Database(arg['database_path']) as database:
            for attr in arg['attributes']:
                database.submit('REPLACE INTO {} (hash, value) VALUES ("{}", "{}")'.format(attr[0], arg['hashvalue'], attr[1]))
    finally:
        mutex.release()


def compute_clause_types(database_path, hashvalue, filename):
    eprint('Computing clause_types for {}'.format(filename))
    file = open_cnf_file(filename, 'rt')
    c_vars = 0
    c_clauses = 0
    c_horn = 0
    c_pos = 0
    c_neg = 0
    for line in file:
        if line.strip() and len(line.strip().split()) > 1:
            parts = line.strip().split()[:-1]
            if parts[0][0] == 'c' or parts[0][0] == 'p' or len(parts) == 0:
                continue
            c_vars = max(c_vars, max(int(part) for part in parts))
            c_clauses += 1
            n_neg = sum(int(part) < 0 for part in parts)
            if n_neg < 2:
                c_horn += 1
                if n_neg == 0:
                    c_pos += 1
            if n_neg == len(parts):
                c_neg += 1
    file.close()
    attributes = [ ('clauses_horn', c_horn), ('clauses_positive', c_pos), ('clauses_negative', c_neg), ('variables', c_vars), ('clauses', c_clauses) ]
    return { 'database_path': database_path, 'hashvalue': hashvalue, 'attributes': attributes }


def compute_sorted_hash(database_path, hashvalue, filename):
    eprint('Computing sorted_hash for {}'.format(filename))
    file = open_cnf_file(filename, 'rb')
    sorted_hash = gbd_hash_sorted(file)
    file.close()
    return { 'database_path': database_path, 'hashvalue': hashvalue, 'attributes': [ ('sorted_hash', sorted_hash) ] }