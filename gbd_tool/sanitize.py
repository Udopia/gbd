from gbd_tool.util import eprint, open_cnf_file
from gbd_tool.db import Database

from gbd_tool.gbd_hash import gbd_hash_sorted

import io
import os
import hashlib
import bz2
import tempfile

import multiprocessing
from multiprocessing import Pool


def sanitize_file(path, h):
    f = open_cnf_file(path, 'rt')
    lc = 0
    preamble_seen = False
    preamble_error = False
    empty_line = False
    fatal_error = False
    decl_clauses = 0
    decl_variables = 0
    num_clauses = 0
    num_variables = 0
    separator = []
    redundancy = []
    for line in f:
        lc = lc + 1
        line = line.strip()
        if not line:
            empty_line = True
        elif line.startswith("p cnf"):
            preamble_seen = True
            header = line.split()
            if len(header) == 4:
                try: 
                    decl_variables = int(header[2])
                    decl_clauses = int(header[3])
                except:
                    preamble_error = True
            else: 
                preamble_error = True
        elif line[0] == 'c' and preamble_seen:
            preamble_error = True
        else:
            if not preamble_error and not preamble_seen:
                preamble_error = True
            try:
                clause = [int(part) for part in line.split()]
                num_clauses = num_clauses + 1
                num_variables = max(num_variables, max([abs(lit) for lit in clause]))
                if 0 in clause[:-1] or clause[-1] != 0:
                    separator.append(lc)
                if len(clause) > len(set(clause)):
                    redundancy.append(lc)
            except:
                fatal_error = True
                break
    f.close()

    preamble_error = preamble_error or decl_variables != num_variables or decl_clauses != num_clauses

    if fatal_error or preamble_error or empty_line or len(separator) > 0 or len(redundancy) > 0:
        log = open("{}.sanitize.log".format(path), 'wt')
        print("Sanitizing {}".format(path), file=log)
        print("Hash {}".format(h), file=log)
        if fatal_error:
            print("Error: clause not readable at line {}".format(lc), file=log)
        if len(redundancy) > 0:
            print("Error: {} clauses with redundant literals".format(len(redundancy)), file=log)
        if len(separator) > 0:
            print("Error: {} misalignments of clause boundary and line break".format(len(separator)), file=log)
        if preamble_error:
            print("Warning: misplaced, missing or erroneous preamble", file=log)
        if empty_line:
            print("Warning: found empty lines", file=log)
        if (preamble_error or empty_line) and not fatal_error:
            print("Fixing: preamble and empty lines", file=log)
            f = open_cnf_file(path, 'rt')
            sani = open("{}.sanitize.cnf".format(path), 'wt')
            done = False
            for line in f:
                if not done and line and line[0] == 'c':
                    sani.print(line)
                elif line and line[0] != 'p':
                    if not done:
                        sani.print("p cnf {} {}".format(num_variables, num_clauses))
                        done = True
                    sani.print(line)
            f.close()
            sani.close()
            log.write("Sanitized {}".format("{}.sanitize.cnf".format(path)), file=log)
        log.close()

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
