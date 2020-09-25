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

__all__ = ['gbd_hash', 'gbd_hash_inner', 'HASH_VERSION']

# Hash-Version 0: initial version (regex based normaliation)
# Hash-Version 1: skip header for normalization (streaming normalization)
# Hash-Version 2: fixed bug in version 1 (do not skip -)
# Hash-Version 3: add trailing zero to last clause if missing

HASH_VERSION = 3
BUFFER_SIZE = io.DEFAULT_BUFFER_SIZE * 16

def gbd_hash(filename):
    file = open_cnf_file(filename, 'rb')
    hashvalue = gbd_hash_inner(io.BufferedReader(file, BUFFER_SIZE))
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
