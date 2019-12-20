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
import hashlib
import io
import lzma
import mmap
import re
import sys

from gbd_tool.util import eprint

__all__ = ['gbd_hash', 'HASH_VERSION']

# Hash-Version 0: initial version (regex based normaliation)
# Hash-Version 1: skip header for normalization (streaming normalization)
# Hash-Version 2: fixed bug in version 1 (do not skip -)
# Hash-Version 3: add trailing zero to last clause if missing

HASH_VERSION = 3

class CNFNormalizerVersion:
    def __init__(self, filename):
        self.space = False
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
        self.m = mmap.mmap(self.f.fileno(), 0, access=mmap.ACCESS_READ)

    def __enter__(self):
        return self

    def read(self):
        eprint("this is a test")
        buf = bytearray()
        byte = b' '
        while len(buf) < 65536 and byte != b'':
            byte = self.m.read(1)
            if byte <= b' ':
                self.space = not self.start  # merge whitespaces
            elif byte == b'c' or byte == b'p':  # exclude comments and header
                while byte != b'\n' or byte != b'\r' or byte != b'':
                    byte = self.m.read(1)
            elif byte >= b'0' and byte <= b'9' or byte == b'-':
                if self.space:
                    buf.append(ord(b' '))  # pending whitespace
                    self.space = False
                buf.append(ord(byte))
                self.start = False
        return buf

    def __exit__(self, exc_type, exc_value, traceback):
        self.f.close()


def gbd_hash(fname):
    hash_md5 = hashlib.md5()
    with CNFNormalizerVersion(fname) as f:
        for chunk in iter(lambda: f.read(), b''):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def hash_hashlist(hashlist):
    hash_md5 = hashlib.md5()
    for entry in hashlist:
        hash_md5.update(entry.encode('utf-8'))
    return hash_md5.hexdigest()
