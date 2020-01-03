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

HASH_VERSION = 2

class CNFNormalizerVersion:
    def __init__(self, filename):
        self.space = False
        self.skip = False
        self.start = True
        if filename.endswith('.cnf.gz'):
            self.f = gzip.open(filename, 'rb')
        elif filename.endswith('.cnf.bz2'):
            self.f = bz2.open(filename, 'rb')
        elif filename.endswith('.cnf.lzma'):
            self.f = lzma.open(filename, 'rb')
        elif filename.endswith('.cnf'):
            self.f = open(filename, 'rb')
        else:
            raise Exception("Unknown CNF file-type")

    def __enter__(self):
        return self

    def read(self):
        buf = bytearray()
        while len(buf) < 65536:
            byte = self.f.read(1)
            if byte == b'':
                return buf  # end of file
            if not self.skip and (byte >= b'0' and byte <= b'9' or byte == b'-'):
                if self.space and not self.start:
                    buf.append(ord(b' '))  # append pending space
                    self.space = False
                buf.append(ord(byte))
                self.start = False
            elif byte == b'c' or byte == b'p':
                self.skip = True  # do not hash comment and header line
            elif byte <= b' ':
                self.space = True  # do not immediately append spaces but remember that there was at least one
                if byte == b'\n' or byte == b'\r':
                    self.skip = False  # comment line ended
        return buf

    def __exit__(self, exc_type, exc_value, traceback):
        self.f.close()


def gbd_hash_old(fname):
    start = time.time()
    hash_md5 = hashlib.md5()
    with CNFNormalizerVersion(fname) as f:
        for chunk in iter(lambda: f.read(), b''):
            hash_md5.update(chunk)
    hash_md5.update(b'\n')
    end = time.time()
    print(end - start)
    return hash_md5.hexdigest()


def gbd_hash(fname):
    Tstart = time.time()
    space = False
    skip = False
    start = True
    if fname.endswith('.cnf.gz'):
        f = gzip.open(fname, 'rb')
    elif fname.endswith('.cnf.bz2'):
        f = bz2.open(fname, 'rb')
    elif fname.endswith('.cnf.lzma'):
        f = lzma.open(fname, 'rb')
    elif fname.endswith('.cnf'):
        f = open(fname, 'rb')
    else:
        raise Exception("Unknown CNF file-type")
    hash_md5 = hashlib.md5()

    for byte in iter(lambda: f.read(1), b''):
        if not skip and (byte >= b'0' and byte <= b'9' or byte == b'-'):
            if space and not start:
                hash_md5.update(b' ')  # append pending space
                space = False
            hash_md5.update(byte)
            start = False
        elif byte == b'c' or byte == b'p':
            skip = True  # do not hash comment and header line
        elif byte <= b' ':
            space = True  # do not immediately append spaces but remember that there was at least one
            if byte == b'\n' or byte == b'\r':
                skip = False  # comment line ended

    hash_md5.update(b'\n')
    f.close()
    
    Tend = time.time()
    eprint("Seconds to hash: {:1.2}".format(Tend - Tstart))
    return hash_md5.hexdigest()

def hash_hashlist(hashlist):
    hash_md5 = hashlib.md5()
    for entry in hashlist:
        hash_md5.update(entry.encode('utf-8'))
    return hash_md5.hexdigest()
