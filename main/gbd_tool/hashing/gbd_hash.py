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

def gbd_hash(filename):
    if filename.endswith('.cnf.gz'):
        file = gzip.open(filename, 'rb')
    elif filename.endswith('.cnf.bz2'):
        file = bz2.open(filename, 'rb')
    elif filename.endswith('.cnf.lzma'):
        file = lzma.open(filename, 'rb')
    elif filename.endswith('.cnf'):
        file = open(filename, 'rb')
    else:
        raise Exception("Unknown CNF file-type")

    eprint("Creating GBD Hash")
    hashvalue = gbd_hash_inner(file)

    file.close()
    return hashvalue
    

def gbd_hash_inner(file):
    Tstart = time.time()
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
        elif byte == b'c' or byte == b'p':
            skip = True  # do not hash comment and header line
        elif byte <= b' ':
            space = True  # do not immediately append spaces but remember that there was at least one
            if skip and (byte == b'\n' or byte == b'\r'):
                skip = False  # comment line ended

    if not blankzero:
        hash_md5.update(b' 0')
    
    Tend = time.time()
    eprint("Seconds to hash: {0:5.2f}".format(Tend - Tstart))
    return hash_md5.hexdigest()


def hash_hashlist(hashlist):
    hash_md5 = hashlib.md5()
    for entry in hashlist:
        hash_md5.update(entry.encode('utf-8'))
    return hash_md5.hexdigest()
