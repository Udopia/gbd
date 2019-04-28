from __future__ import print_function

import bz2
import gzip
import hashlib
import io
import lzma
import mmap
import re
import sys

__all__ = ['gbd_hash', 'HASH_VERSION']

HASH_VERSION = 2


# still includes header
class CNFNormalizerVersion0:
    def __init__(self, filename):
        if filename.endswith('.cnf.gz'):
            self.f = gzip.open(filename, 'rt')
        elif filename.endswith('.cnf.bz2'):
            self.f = bz2.open(filename, 'rt')
        elif filename.endswith('.cnf'):
            self.f = open(filename, 'rt')
        else:
            raise Exception("Unknown CNF file-type")
        self.bytes = io.BytesIO()

    def __enter__(self):
        return self

    def read(self, nbytes):
        text = self.f.readline()
        while text.lstrip().startswith("c") or text == '\n':
            text = self.f.readline()
        text = re.sub('[\t ]+', ' ', text.lstrip())
        text = re.sub(' 0 ', ' 0', text)
        pos = self.bytes.tell()
        self.bytes.write(text.encode())
        self.bytes.seek(pos)
        return self.bytes.read(nbytes)

    def __exit__(self, exc_type, exc_value, traceback):
        self.f.close()


# improved performance through streaming and removed header (buggy, skips '-')
class CNFNormalizerVersion1:
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
        self.m = mmap.mmap(self.f.fileno(), 0, access=mmap.ACCESS_READ)

    def __enter__(self):
        return self

    def read(self):
        buf = bytearray()
        while len(buf) < 8192:
            byte = self.m.read(1)
            if byte == b'':
                return buf  # end of file
            if not self.skip and byte >= b'0' and byte <= b'9':
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


# like version 1, but with bug-fix
class CNFNormalizerVersion2:
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
        self.m = mmap.mmap(self.f.fileno(), 0, access=mmap.ACCESS_READ)

    def __enter__(self):
        return self

    def read(self):
        buf = bytearray()
        while len(buf) < 8192:
            byte = self.m.read(1)
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


def gbd_hash_version0(fname):
    hash_md5 = hashlib.md5()
    with CNFNormalizerVersion0(fname) as f:
        for chunk in iter(lambda: f.read(4096), b''):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def gbd_hash_version1(fname):
    hash_md5 = hashlib.md5()
    with CNFNormalizerVersion1(fname) as f:
        for chunk in iter(lambda: f.read(), b''):
            hash_md5.update(chunk)
    hash_md5.update(b'\n')
    return hash_md5.hexdigest()


def gbd_hash_version2(fname):
    hash_md5 = hashlib.md5()
    with CNFNormalizerVersion2(fname) as f:
        for chunk in iter(lambda: f.read(), b''):
            hash_md5.update(chunk)
    hash_md5.update(b'\n')
    return hash_md5.hexdigest()


def gbd_hash(fname, version=HASH_VERSION):
    if version == 0:
        return gbd_hash_version0(fname)
    elif version == 1:
        return gbd_hash_version1(fname)
    else:
        return gbd_hash_version2(fname)


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def hash_hashlist(hashlist):
    hash_md5 = hashlib.md5()
    for entry in hashlist:
        hash_md5.update(entry.encode('utf-8'))
    return hash_md5.hexdigest()
