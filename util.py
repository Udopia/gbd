from __future__ import print_function
import sys
import os
import hashlib
import gzip
import bz2
import lzma
import io
import re
import mmap

__all__ = ['gbd_hash', 'eprint']

class CNFNormalizerVersion0:
  def __init__ (self, filename):
    if filename.endswith('.cnf.gz'):
      self.f = gzip.open(filename, 'rt')
    elif filename.endswith('.cnf.bz2'):
      self.f = bz2.open(filename, 'rt')
    elif filename.endswith('.cnf'):
      self.f = open(filename, 'rt')
    else:
      raise Exception("Unknown CNF file-type")
    self.bytes = io.BytesIO()
  def __enter__ (self):
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
  def __exit__ (self, exc_type, exc_value, traceback):
    self.f.close()

class CNFNormalizerVersion1:
  def __init__ (self, filename):
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
      raise Exception("Skipping {}. Unregistered file suffix.".format(filename))
  def __enter__ (self):
    return self
  def read(self):
    buf = bytearray()
    while len(buf) < 8192:
      byte = self.f.read(1)
      if byte == b'':
        return buf
      if not self.skip and byte >= b'0' and byte <= b'9':
        if self.space and not self.start:
          buf.append(ord(b' '))
          self.space = False
        buf.append(ord(byte))
        self.start = False
      elif byte == b'c' or byte == b'p':
        self.skip = True
      elif byte <= b' ':
        self.space = True
        if byte == b'\n' or byte == b'\r':
          self.skip = False
    return buf
  def __exit__ (self, exc_type, exc_value, traceback):
    self.f.close()

class CNFNormalizerVersion2:
  def __init__ (self, filename):
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
  def __enter__ (self):
    return self
  def read(self):
    buf = bytearray()
    while len(buf) < 8192:
      byte = self.m.read(1)
      if byte == b'':
        return buf
      if not self.skip and byte >= b'0' and byte <= b'9':
        if self.space and not self.start:
          buf.append(ord(b' '))
          self.space = False
        buf.append(ord(byte))
        self.start = False
      elif byte == b'c' or byte == b'p':
        self.skip = True
      elif byte <= b' ':
        self.space = True
        if byte == b'\n' or byte == b'\r':
          self.skip = False
    return buf
  def __exit__ (self, exc_type, exc_value, traceback):
    self.f.close()

def gbd_hash_version0(fname):
  hash_md5 = hashlib.md5()
  with CNFNormalizerVersion0(fname) as f:
    for chunk in iter(lambda: f.read(4096), b''):
      hash_md5.update(chunk)
  return hash_md5.hexdigest()

def gbd_hash_version1(fname):
  hash_md5 = hashlib.md5()
  with CNFNormalizerVersion2(fname) as f:
    for chunk in iter(lambda: f.read(), b''):
      #print(':'.join('{:02x}'.format(x) for x in chunk))
      hash_md5.update(chunk)
  hash_md5.update(b'\n')
  return hash_md5.hexdigest()

def gbd_hash(fname, version):
  if version == 0:
    return gbd_hash_version0(fname)
  else:
    return gbd_hash_version1(fname)

def eprint(*args, **kwargs):
  print(*args, file=sys.stderr, **kwargs)
