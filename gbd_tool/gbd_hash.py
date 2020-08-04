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

import io
import hashlib
import re
import sys
import time
import array

from gbd_tool.util import eprint, open_cnf_file

__all__ = ['gbd_hash', 'HASH_VERSION']

# Hash-Version 0: initial version (regex based normaliation)
# Hash-Version 1: skip header for normalization (streaming normalization)
# Hash-Version 2: fixed bug in version 1 (do not skip -)
# Hash-Version 3: add trailing zero to last clause if missing

HASH_VERSION = 3


def gbd_hash(filename, sorted=False):
    file = open_cnf_file(filename, 'rb')

    if sorted:
        hashvalue = gbd_hash_sorted(file)
    else:
        hashvalue = gbd_hash_inner(file)

    file.close()
    return hashvalue

def gbd_hash_inner(file):
    #Tstart = time.time()
    space = False
    skip = False
    start = True
    cldelim = True
    hash_md5 = hashlib.md5()

    for byte in iter(lambda: file.read(1), b''):
        if not skip and (byte >= b'0' and byte <= b'9' or byte == b'-'):
            cldelim = byte == b'0' and (space or start)
            start = False
            if space:
                space = False
                hash_md5.update(b' ')
            hash_md5.update(byte)
        elif byte <= b' ':
            space = not start # remember non-leading space characters 
            skip = skip and byte != b'\n' and byte != b'\r' # comment line ended
        else: #byte == b'c' or byte == b'p':
            skip = True  # do not hash comment and header line

    if not cldelim:
        hash_md5.update(b' 0')

    #Tend = time.time()
    #eprint("Seconds to hash: {0:5.2f}".format(Tend - Tstart))
    return hash_md5.hexdigest()


def gbd_hash_sorted(file):
    #print(file)
    clauses = []
    clause = []
    literal = 0
    
    space = False
    skip = False
    sign = False
    eprint("Opened it. Reading...")
    for byte in iter(lambda: file.read(1), b''):
        if not skip and (byte >= b'0' and byte <= b'9' or byte == b'-'):
            if space:
                space = False
                if literal != 0:
                    clause.append(literal)
                    literal = 0
                    sign = False
                if byte == b'0':
                    clause.sort()
                    clauses.append(array.array('i', clause))
                    clause.clear()
            if byte != b'0':
                if byte == b'-':
                    sign = True
                elif sign:
                    literal = literal * 10 - int(byte)
                else:
                    literal = literal * 10 + int(byte)
        elif byte <= b' ':
            space = True  # remember whitespace
            if skip and (byte == b'\n' or byte == b'\r'):
                skip = False  # comment line ended
        elif byte == b'c' or byte == b'p':
            skip = True  # skip comment and header line
    
    if literal > 0:
        clause.append(literal)
    if len(clause):
        clause.sort()
        clauses.append(array.array('i', clause))
        clause.clear()

    eprint("Read it all. Sorting...")
    clauses.sort(key = lambda clause: (len(clause), clause))
    eprint("Sorted it all. Hashing...")

    hash_md5 = hashlib.md5()
    start = True
    for clause in clauses:
        if not start:
            hash_md5.update(b' ')
            #eprint(' ')
        hash_md5.update(b' '.join([str(num).encode('utf-8') for num in clause]))
        hash_md5.update(b' 0')
        #eprint(str(' '.join([str(num) for num in clause+[0]])))
        start = False

    return hash_md5.hexdigest()
