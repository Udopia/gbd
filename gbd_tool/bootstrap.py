from gbd_tool.util import eprint, open_cnf_file
from gbd_tool.error import GbdApiError
from gbd_tool.db import Database

import io
import os
import hashlib
import bz2
import tempfile

import multiprocessing
from multiprocessing import Pool, Lock

mutex = Lock()

def bootstrap(api, database, named_algo, hashes, jobs):
    resultset = api.query_search(None, hashes, ["local"])
    if named_algo == 'clause_types':
        for table in [ "clauses_horn", "clauses_positive", "clauses_negative", "variables", "clauses" ]:
            if not api.feature_exists(table):
                api.create_feature(table, "empty")
        schedule_bootstrap(api, jobs, resultset, compute_clause_types)
    elif named_algo == 'degree_sequence_hash':
        api.create_feature("degree_sequence_hash", "empty")
        schedule_bootstrap(api, jobs, resultset, compute_degree_sequence_hash)
    elif named_algo == 'sanitation_info':
        api.create_feature("sanitation_info")
        schedule_bootstrap(api, jobs, resultset, compute_cnf_sanitation_info)
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
                raise GbdApiError("clause is empty: {}".format(line))
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

def compute_cnf_sanitation_info(hashvalue, filename):
    eprint('Computing sanitiation info for {}'.format(filename))
    f = open_cnf_file(filename, 'rt')
    attributes = [ ('INSERT', 'sanitation_info', 'checked') ]
    lc = 0
    preamble = False
    decl_clauses = 0
    decl_variables = 0
    num_clauses = 0
    num_variables = 0
    for line in f:
        lc = lc + 1
        line = line.strip()
        if not line:
            attributes.append(('INSERT', 'sanitation_info', "Warning: empty line {}".format(lc)))
        elif line.startswith("p cnf"):
            if preamble:
                attributes.append(('INSERT', 'sanitation_info', "Warning: more than one preamble"))
            preamble = True
            header = line.split()
            if len(header) == 4:
                try: 
                    decl_variables = int(header[2])
                    decl_clauses = int(header[3])
                except:
                    attributes.append(('INSERT', 'sanitation_info', "Warning: unable to read preamble"))
            else: 
                attributes.append(('INSERT', 'sanitation_info', "Warning: unable to read preamble"))
        elif line[0] == 'c' and preamble:
            attributes.append(('INSERT', 'sanitation_info', "Warning: comment after preamble in line {}".format(lc)))
        else:
            if not preamble:
                attributes.append(('INSERT', 'sanitation_info', "Warning: preamble missing"))
                preamble = True
            try:
                clause = [int(part) for part in line.split()]
                num_clauses = num_clauses + 1
                num_variables = max(num_variables, max([abs(lit) for lit in clause]))
                if 0 in clause[:-1]:
                    attributes.append(('INSERT', 'sanitation_info', "Error: more than one clause in line {}".format(lc)))
                if clause[-1] != 0:
                    attributes.append(('INSERT', 'sanitation_info', "Error: clause not terminated in line {}".format(lc)))
                if len(clause) > len(set(clause)):
                    attributes.append(('INSERT', 'sanitation_info', "Error: redundant literals in line {}".format(lc)))
            except Exception as e:
                attributes.append(('INSERT', 'sanitation_info', "Error: clause not readable in line {}, {}".format(lc, e)))
                break
    f.close()

    if decl_variables != num_variables: 
        attributes.append(('INSERT', 'sanitation_info', "Warning: {} variables declared, but found {} variables".format(decl_variables, num_variables)))
        
    if decl_clauses != num_clauses:
        attributes.append(('INSERT', 'sanitation_info', "Warning: {} variables declared, but found {} variables".format(decl_clauses, num_clauses)))

    return { 'hashvalue': hashvalue, 'attributes': attributes }