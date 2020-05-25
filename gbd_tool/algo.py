from gbd_tool.util import eprint, open_cnf_file
from gbd_tool.db import Database

from gbd_tool.gbd_hash import gbd_hash_sorted

import io
import hashlib
import bz2

import multiprocessing
from multiprocessing import Pool, Lock

mutex = Lock()

def sanitize_file(path, h):
    spath = "{}.san.cnf".format(path)
    lpath = "{}.san.log".format(path)
    f = open_cnf_file(path, 'rt')
    sf = open(spath, 'wt')
    lf = open(lpath, 'wt')
    print("Sanitizing {}".format(path), file=lf)
    print("Sanitized {}".format(spath), file=lf)
    print("Hash {}".format(h), file=lf)
    lc = 0
    preamble_done = False
    skipped_misplaced_comments = 0
    decl_clauses = 0
    decl_variables = 0
    num_clauses = 0
    num_variables = 0
    for line in f:
        lc = lc + 1
        line = line.strip()
        if not line:
            print("Fixed: skipping empty line at {}".format(lc), file=lf)
        elif line.startswith("p cnf"):
            print(line, file=sf)
            header = line.split()
            if len(header) != 4:
                print("Warning: invalid preamble at {}".format(lc), file=lf)
            else:
                try: 
                    decl_variables = int(header[2])
                    decl_clauses = int(header[3])
                    preamble_done = True
                except:
                    print("Warning: invalid preamble at {}".format(lc), file=lf)
        elif line[0] == 'c':
            if preamble_done:
                skipped_misplaced_comments = skipped_misplaced_comments + 1
            else:
                print(line, file=sf)
        else:
            if not preamble_done:
                print("Warning: missing preamble at {}".format(lc), file=lf)
                preamble_done = True
            try:
                print(line, file=sf)
                clause = [int(part) for part in line.split()]
                num_clauses = num_clauses + 1
                num_variables = max(num_variables, max([abs(lit) for lit in clause]))
                if 0 in clause[:-1]:
                    print("Warning: more than one clause per line at {}".format(lc), file=lf)
                if clause[-1] != 0:
                    print("Warning: clause not delimited by 0 at {}".format(lc), file=lf)
                if len(clause) > len(set(clause)):
                    print("Warning: clause contains duplicate literal at {}".format(lc), file=lf) 
            except:
                print("Warning: unreadable literal in clause at {}".format(lc), file=lf)
    if skipped_misplaced_comments > 0:
        print("Fixed: skipped {} misplaced comment after preamble".format(skipped_misplaced_comments), file=lf)
    if num_variables > decl_variables:
        print("Warning: found more variables than declared", file=lf)
    if decl_clauses > num_clauses:
        print("Warning: found more clauses than declared", file=lf)
    if decl_clauses < num_clauses:
        print("Warning: found less clauses than declared", file=lf)
    print("Variables {}".format(num_variables), file=lf)
    print("Clauses {}".format(num_clauses), file=lf)
    f.close()

def sanitize(api, database, hashes, jobs):
    if jobs == 1:
        for h in hashes:
            paths = api.search("local", h)
            path = paths.pop()
            sanitize_file(path, h)
    else:
        pool = Pool(min(multiprocessing.cpu_count(), jobs))
        for h in hashes:
            paths = api.search("local", h)
            if len(paths):
                path = paths.pop()            
                eprint("Sanitizing {} {}".format(h, path))
                pool.apply_async(sanitize_file, args=(path, h))
            else:
                eprint("No file for hash {}".format(h))
        pool.close()
        pool.join() 


def bootstrap(api, database, named_algo, jobs):
    if named_algo == 'clause_types':
        api.add_attribute_group("clauses_horn", "integer", 0)
        api.add_attribute_group("clauses_positive", "integer", 0)
        api.add_attribute_group("clauses_negative", "integer", 0)
        api.add_attribute_group("variables", "integer", 0)
        api.add_attribute_group("clauses", "integer", 0)
        resultset = api.query_search("clauses = 0", ["local"])
        schedule_bootstrap(database.path, jobs, resultset, compute_clause_types)
    elif named_algo == 'degree_sequence_hash':
        api.add_attribute_group("degree_sequence_hash", "text", "empty")
        resultset = api.query_search("degree_sequence_hash = empty", ["local"])
        schedule_bootstrap(database.path, jobs, resultset, compute_degree_sequence_hash)
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
    attributes = [ ('clauses_horn', c_horn), ('clauses_positive', c_pos), ('clauses_negative', c_neg), ('variables', c_vars), ('clauses', c_clauses) ]
    return { 'database_path': database_path, 'hashvalue': hashvalue, 'attributes': attributes }


def compute_degree_sequence_hash(database_path, hashvalue, filename):
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

    return { 'database_path': database_path, 'hashvalue': hashvalue, 'attributes': [ ('degree_sequence_hash', hash_md5.hexdigest()) ] }