# Global Benchmark Database (GBD)
# Copyright (C) 2019 Markus Iser, Luca Springer, Karlsruhe Institute of Technology (KIT)
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

import bz2
import gzip
import lzma
import hashlib
import io
import re
import sys
import time

from gbd_tool.util import eprint

__all__ = ['gbd_hash', 'HASH_VERSION']

# Hash-Version 0: initial version (regex based normaliation)
# Hash-Version 1: skip header for normalization (streaming normalization)
# Hash-Version 2: fixed bug in version 1 (do not skip -)
# Hash-Version 3: add trailing zero to last clause if missing

HASH_VERSION = 3


def gbd_hash(filename, sorted=False):
    if filename.endswith('.cnf.gz'):
        file = gzip.open(filename, 'rb')
    elif filename.endswith('.cnf.bz2'):
        file = bz2.open(filename, 'rb')
    elif filename.endswith('.cnf.lzma'):
        file = lzma.open(filename, 'rb')
    elif filename.endswith('.cnf'):
        file = open(filename, 'rb')
    else:
        raise Exception("Unknown File Extension. Use .cnf, .cnf.bz2, .cnf.lzma, or .cnf.gz")

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
    blankzero = False
    hash_md5 = hashlib.md5()

    for byte in iter(lambda: file.read(1), b''):
        if not skip and (byte >= b'0' and byte <= b'9' or byte == b'-'):
            blankzero = space and byte == b'0'
            if space and not start:
                hash_md5.update(b' ')  # append pending space
                space = False
            hash_md5.update(byte)
            start = False
        elif byte <= b' ':
            space = True  # do not immediately append spaces but remember that there was at least one
            if skip and (byte == b'\n' or byte == b'\r'):
                skip = False  # comment line ended
        elif byte == b'c' or byte == b'p':
            skip = True  # do not hash comment and header line

    if not blankzero:
        hash_md5.update(b' 0')

    #Tend = time.time()
    #eprint("Seconds to hash: {0:5.2f}".format(Tend - Tstart))
    return hash_md5.hexdigest()


def gbd_hash_sorted(file):
    #print(file)
    clauses = []
    clause = []
    literal = b''
    
    space = False
    skip = False
    for byte in iter(lambda: file.read(1), b''):
        if not skip and (byte >= b'0' and byte <= b'9' or byte == b'-'):
            if space:
                space = False
                if len(literal):
                    clause.append(int(literal))
                    literal = b''
                if byte == b'0':
                    clauses.append(sorted(clause))
                    clause = []
                else:
                    literal = literal + byte
            else:
                literal = literal + byte
        elif byte <= b' ':
            space = True  # remember whitespace
            if skip and (byte == b'\n' or byte == b'\r'):
                skip = False  # comment line ended
        elif byte == b'c' or byte == b'p':
            skip = True  # skip comment and header line
    
    if len(literal):
        clause.append(int(literal))
    if len(clause):
        clauses.append(sorted(clause))
        clause = []

    clauses.sort(key = lambda clause: (len(clause), clause))

    hash_md5 = hashlib.md5()
    start = True
    for clause in clauses:
        if not start:
            hash_md5.update(b' ')
        hash_md5.update(b' '.join([str(num).encode('utf-8') for num in clause+[0]]))
        start = False

    return hash_md5.hexdigest()
