from gbd_tool.util import eprint
from gbd_tool.db import Database

import bz2
import gzip
import lzma

import multiprocessing
from multiprocessing import Pool, Lock

mutex = Lock()


def safe_horn_locked(arg):
    mutex.acquire()
    try:
        # create new connection from old one due to limitations of multi-threaded use (cursor initialization issue)
        with Database(arg['database_path']) as database:
            database.submit('REPLACE INTO {} (hash, value) VALUES ("{}", "{}")'.format("clauses_horn", arg['hashvalue'], arg['c_horn']))
            database.submit('REPLACE INTO {} (hash, value) VALUES ("{}", "{}")'.format("clauses_positive", arg['hashvalue'], arg['c_pos']))
            database.submit('REPLACE INTO {} (hash, value) VALUES ("{}", "{}")'.format("clauses_negative", arg['hashvalue'], arg['c_neg']))
            database.submit('REPLACE INTO {} (hash, value) VALUES ("{}", "{}")'.format("variables", arg['hashvalue'], arg['c_vars']))
            database.submit('REPLACE INTO {} (hash, value) VALUES ("{}", "{}")'.format("clauses", arg['hashvalue'], arg['c_clauses']))
    finally:
        mutex.release()

def compute_horn(database_path, hashvalue, filename):
    file = None
    if filename.endswith('.cnf.gz'):
        file = gzip.open(filename, 'rt')
    elif filename.endswith('.cnf.bz2'):
        file = bz2.open(filename, 'rt')
    elif filename.endswith('.cnf.lzma'):
        file = lzma.open(filename, 'rt')
    elif filename.endswith('.cnf'):
        file = open(filename, 'rt')
    else:
        raise Exception("Unknown CNF file-type")
    c_vars = 0
    c_clauses = 0
    c_horn = 0
    c_pos = 0
    c_neg = 0
    eprint('Computing bootstrap attributes for {}'.format(filename))
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
    return { 'database_path': database_path, 'hashvalue': hashvalue, 'c_horn': c_horn, 'c_pos': c_pos, 'c_neg': c_neg, 'c_vars': c_vars, 'c_clauses': c_clauses }

def algo_horn(api, database, jobs):
    api.add_attribute_group("clauses_horn", "integer", 0)
    api.add_attribute_group("clauses_positive", "integer", 0)
    api.add_attribute_group("clauses_negative", "integer", 0)
    api.add_attribute_group("variables", "integer", 0)
    api.add_attribute_group("clauses", "integer", 0)

    pool = Pool(min(multiprocessing.cpu_count(), jobs))
    resultset = api.query_search("clauses = 0", ["benchmarks"])
    for result in resultset:
        hashvalue = result[0].split(',')[0]
        filename = result[1].split(',')[0]
        eprint('Scheduling bootstrap for {}'.format(filename))
        handler = pool.apply_async(compute_horn, args=(database.path, hashvalue, filename), callback=safe_horn_locked)
        #handler.get()
    pool.close()
    pool.join() 