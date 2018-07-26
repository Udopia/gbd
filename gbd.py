#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sqlite3 as lite
import sys
import os
import hashlib
import argparse
import re

from db import Database, DatabaseException, HASH_VERSION
import groups
import search
import tags
from util import *

from os.path import realpath, dirname, join, isfile
DEFAULT_DATABASE = join(dirname(realpath(__file__)), 'local.db')

def iface_hash(args):
  eprint('Hashing Benchmark: {}'.format(args.path))
  print(gbd_hash(args.path, HASH_VERSION))

def init(args):
  if (args.path is not None):
    eprint('Removing invalid benchmarks from path: {}'.format(args.path))
    remove_benchmarks(args.db)
    eprint('Registering benchmarks from path: {}'.format(args.path))
    register_benchmarks(args.db, args.path, HASH_VERSION)
  else:
    Database(args.db)

def remove_benchmarks(db):
  with Database(db) as database:
    paths = database.value_query("SELECT value FROM benchmarks")
    for p in paths:
      if not isfile(p):
        eprint("Problem '{}' not found. Removing...".format(p))
        database.submit("DELETE FROM benchmarks WHERE value='{}'".format(p))

def register_benchmarks(db, benchmark_root, hash_version):
  for root, dirnames, filenames in os.walk(benchmark_root):
    for filename in filenames:
      path = os.path.join(root, filename)
      eprint('Found {}'.format(path))
      try:
        with Database(db) as database:
          hashes = database.value_query("SELECT hash FROM benchmarks WHERE value = '{}'".format(path))
          if len(hashes) is not 0:
            eprint('Problem {} already hashed'.format(path))
            continue
        hashvalue = gbd_hash(path, hash_version)
        with Database(db) as database:
          tags.add_benchmark(database, hashvalue, path)
      except DatabaseException as e:
        eprint(e)
        return
      except UnicodeDecodeError as e:
        eprint('Skipping file due to decoding error: {}'.format(e))
        continue

# entry for modify command
def group(args):
  if args.name.startswith("__"):
    eprint("Names starting with '__' are reserved for system tables")
    return
  with Database(args.db) as database:
    if (args.name in groups.reflect(database)):
      eprint("Group {} does already exist".format(args.name))
      return
    if args.remove and confirm("Delete group '{}'?".format(args.name)):
      groups.remove(database, args.name)
    elif args.clear and confirm("Clear group '{}'?".format(args.name)):
      groups.clear(database, args.name)
    else:
      eprint("Adding or modifying group {}, unique {}, type {}, default-value {}".format(args.name, args.unique, args.type, args.value))
      groups.add(database, args.name, args.unique, args.type, args.value)

# entry for query command
def query(args):
  hashes = {}

  with Database(args.db) as database:
    if (args.query is None):
      hashes = search.find_hashes(database, 'benchmarks')
    else:
      hashes = search.find_hashes_by_query(database, args.query)

  if (args.union):
    inp = read_hashes_from_stdin()
    hashes.update(inp)
  elif (args.intersection):
    inp = read_hashes_from_stdin()
    hashes.intersection_update(inp)

  print(*hashes, sep='\n')

# associate a tag with a hash-value
def tag(args):
  hashes = read_hashes_from_stdin()
  with Database(args.db) as database:
    if args.remove and confirm("Delete tag '{}' from '{}'?".format(args.value, args.name)):
      for hash in hashes:
        tags.remove_tag(database, args.name, args.value, hash)
    else:
      for hash in hashes:
        tags.add_tag(database, args.name, args.value, hash)

def resolve(args):
  hashes = read_hashes_from_stdin()
  with Database(args.db) as database:
    for hash in hashes:
      out = []
      for name in args.name:
        resultset = sorted(search.resolve(database, name, hash))
        resultset = [str(element) for element in resultset]
        if (name == 'benchmarks' and args.pattern is not None):
          res = [k for k in resultset if args.pattern in k]
          resultset = res
        if (len(resultset) > 0):
          if (args.collapse):
            out.append(resultset[0])
          else:
            out.append(' '.join(resultset))
      print(','.join(out))

def read_hashes_from_stdin():
  hashes = set()
  try:
    while True:
      line = sys.stdin.readline()
      if len(line.strip()) == 0:
        return hashes
      hashes.add(line.strip())
  except KeyboardInterrupt:
    return hashes
  return hashes

def reflection(args):
  database = Database(args.db)
  if (args.name is not None):
    if (args.values):
      print(*groups.reflect_tags(database, args.name), sep='\n')
    else:
      print('name: {}'.format(args.name))
      print('type: {}'.format(groups.reflect_type(database, args.name)))
      print('uniqueness: {}'.format(groups.reflect_unique(database, args.name)))
      print('default value: {}'.format(groups.reflect_default(database, args.name)))
      print('number of entries: {}'.format(*groups.reflect_size(database, args.name)))
  else:
    if not database.has_version_entry():
      print("DB '{}' has no version entry".format(args.db))

    print("DB '{}' was created with version: {} and HASH version: {}".format(args.db, database.get_version(), database.get_hash_version()))
    print("Found tables:")
    print(*groups.reflect(database))

def confirm(prompt=None, resp=False):
    """
    prompts for yes or no response from the user. Returns True for yes and False for no.
    'resp' should be set to the default value assumed by the caller when user simply types ENTER.
    """
    if prompt is None:
        prompt = 'Confirm'

    if resp:
        prompt = '%s [%s]|%s: ' % (prompt, 'y', 'n')
    else:
        prompt = '%s [%s]|%s: ' % (prompt, 'n', 'y')

    while True:
        ans = input(prompt)
        if not ans:
            return resp
        if ans not in ['y', 'Y', 'n', 'N']:
            print('please enter y or n.')
            continue
        if ans == 'y' or ans == 'Y':
            return True
        if ans == 'n' or ans == 'N':
            return False

# define directory type for argparse
def directory_type(dir):
  if not os.path.isdir(dir):
    raise argparse.ArgumentTypeError('{0} is not a directory'.format(dir))
  if os.access(dir, os.R_OK):
    return dir
  else:
    raise argparse.ArgumentTypeError('{0} is not readable'.format(dir))

def file_type(path):
  if not os.path.isfile(path):
    raise argparse.ArgumentTypeError('{0} is not a regular file'.format(path))
  if os.access(path, os.R_OK):
    return path
  else:
    raise argparse.ArgumentTypeError('{0} is not readable'.format(path))

def column_type(s):
  pat = re.compile(r"[a-zA-Z][a-f0-9A-F_]*")
  if not pat.match(s):
    raise argparse.ArgumentTypeError('group-name:{0} does not match regular expression {1}'.format(s, pat.pattern))
  return s

def main():
  parser = argparse.ArgumentParser(description='Access and maintain the global benchmark database.')

  parser.add_argument('-d', "--db", help='Specify database to work with', default=DEFAULT_DATABASE, nargs='?')

  subparsers = parser.add_subparsers(help='Available Commands:')

  parser_init = subparsers.add_parser('init', help='Initialize Database')
  parser_init.add_argument('-p', '--path', type=directory_type, help="Path to benchmarks")
  parser_init.add_argument('-r', '--remove', help='Remove all problems with invalid path', action='store_true')
  parser_init.set_defaults(func=init)

  parser_hash = subparsers.add_parser('hash', help='Print hash for a single file')
  parser_hash.add_argument('-p', '--path', type=file_type, help="Path to one benchmark", required=True)
  parser_hash.set_defaults(func=iface_hash)

  # define reflection
  parser_reflect = subparsers.add_parser('reflect', help='Reflection, Display Groups')
  parser_reflect.add_argument('name', type=column_type, help='Display Details on Group', nargs='?')
  parser_reflect.add_argument('-v', '--values', action='store_true', help='Display Distinct Values of Group')
  parser_reflect.set_defaults(func=reflection)

  # define create command sub-structure
  parser_group = subparsers.add_parser('group', help='Create or modify an attribute group')
  parser_group.add_argument('name', type=column_type, help='Name of group to create (or modify)')
  parser_group.add_argument('-v', '--value', help='Specify a default value for the group (default: None)')
  parser_group.add_argument('-u', '--unique', action='store_true', help='Specify if the group stores unique or several attributes per benchmark (default: false)')
  parser_group.add_argument('-t', '--type', help='Specify the value type of the group (default: text)', default="text", choices=['text', 'integer', 'real'])
  parser_group.add_argument('-r', '--remove', action='store_true', help='If group exists: remove the group with the specified name')
  parser_group.add_argument('-c', '--clear', action='store_true', help='If group exists: remove all values in the group with the specified name')
  parser_group.set_defaults(func=group)

  # define set command sub-structure
  parser_tag = subparsers.add_parser('tag', help='Associate attribues with benchmarks (hashes read line-wise from stdin)')
  parser_tag.add_argument('name', type=column_type, help='Name of attribute group')
  parser_tag.add_argument('-v', '--value', help='Attribute value', required=True)
  parser_tag.add_argument('-r', '--remove', action='store_true', help='Remove attribute from hashes if present, instead of adding it')
  parser_tag.set_defaults(func=tag)

  # define find command sub-structure
  parser_query = subparsers.add_parser('query', help='Query the benchmark database')
  parser_query.add_argument('query', help='Specify a query-string (e.g. "variables > 100 and path like %%mp1%%")', nargs='?')
  parser_query.add_argument('-u', '--union', help='Read hashes from stdin and create union with query results', action='store_true')
  parser_query.add_argument('-i', '--intersection', help='Read hashes from stdin and create intersection with query results', action='store_true')
  parser_query.set_defaults(func=query)

  # define resolve command
  parser_resolve = subparsers.add_parser('resolve', help='Resolve Hashes')
  parser_resolve.add_argument('name', type=column_type, help='Name of group to resolve against', default=["benchmarks"], nargs='*')
  parser_resolve.add_argument('-c', '--collapse', action='store_true', help='Show only one representative per hash')
  parser_resolve.add_argument('-p', '--pattern', help='Substring that must occur in path')
  parser_resolve.set_defaults(func=resolve)

  # evaluate arguments
  if (len(sys.argv) > 1):
    args = parser.parse_args()
    args.func(args)
  else:
    parser.print_help()

if __name__ == '__main__': main()
