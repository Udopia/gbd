#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys
import os
import argparse
import re

from main.core.database import groups, tags, search
from main.core import import_data

from main.core.database.db import Database
from main.core.hashing.gbd_hash import gbd_hash
from main.core.util import eprint, read_hashes, confirm

from os.path import realpath, dirname, join

local_db_path = join(dirname(realpath(__file__)), 'local.db')
DEFAULT_DATABASE = os.environ.get('GBD_DB', local_db_path)


def cli_hash(args):
    eprint('Hashing Benchmark: {}'.format(args.path))
    print(gbd_hash(args.path))


def cli_import(args):
    eprint('Importing Data from CSV-File: {}'.format(args.path))
    with Database(args.db) as database:
        import_data.import_csv(database, args.path, args.key, args.source, args.target)


def cli_init(args):
    if (args.path is not None):
        eprint('Removing invalid benchmarks from path: {}'.format(args.path))
        tags.remove_benchmarks(args.db)
        eprint('Registering benchmarks from path: {}'.format(args.path))
        tags.register_benchmarks(args.db, args.path)
    else:
        Database(args.db)


# entry for modify command
def cli_group(args):
    if args.name.startswith("__"):
        eprint("Names starting with '__' are reserved for system tables")
        return
    with Database(args.db) as database:
        if (args.name in groups.reflect(database) and not args.remove and not args.clear):
                eprint("Group {} does already exist".format(args.name))
        elif not args.remove and not args.clear:
            eprint("Adding or modifying group '{}', unique {}, type {}, default-value {}".format(args.name, args.unique
                                                                                                 is not None,
                                                                                               args.type, args.unique))
            groups.add(database, args.name, args.unique is not None, args.type, args.unique)
            return
        if not (args.name in groups.reflect(database)):
            eprint("Group '{}' does not exist".format(args.name))
            return
        if args.remove and confirm("Delete group '{}'?".format(args.name)):
            groups.remove(database, args.name)
        else:
            if args.clear and confirm("Clear group '{}'?".format(args.name)):
                groups.clear(database, args.name)
        return


# entry for query command
def cli_query(args):
    hashes = {}

    with Database(args.db) as database:
        if (args.query is None):
            hashes = search.find_hashes(database)
        else:
            hashes = search.find_hashes(database, args.query)

    if (args.union):
        inp = read_hashes()
        hashes.update(inp)
    elif (args.intersection):
        inp = read_hashes()
        hashes.intersection_update(inp)

    print(*hashes, sep='\n')


# associate a tag with a hash-value
def cli_tag(args):
    hashes = read_hashes()
    with Database(args.db) as database:
        if args.remove and (args.force or confirm("Delete tag '{}' from '{}'?".format(args.value, args.name))):
            for hash in hashes:
                tags.remove_tag(database, args.name, args.value, hash)
        else:
            for hash in hashes:
                tags.add_tag(database, args.name, args.value, hash, args.force)


def cli_resolve(args):
    hashes = read_hashes()
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


def cli_reflection(args):
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
        print("DB '{}' was created with version: {} and HASH version: {}".format(args.db, database.get_version(),
                                                                                 database.get_hash_version()))
        print("Found tables:")
        print(*groups.reflect(database))


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
    pat = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]*$")
    if not pat.match(s):
        raise argparse.ArgumentTypeError('group-name:{0} does not match regular expression {1}'.format(s, pat.pattern))
    return s


def main():
    parser = argparse.ArgumentParser(description='Access and maintain the global benchmark database.')

    parser.add_argument('-d', "--db", help='Specify database to work with', default=DEFAULT_DATABASE, nargs='?')

    subparsers = parser.add_subparsers(help='Available Commands:')

    parser_init = subparsers.add_parser('init', help='Initialize Database')
    parser_init.add_argument('path', type=directory_type, help="Path to benchmarks")
    parser_init.add_argument('-r', '--remove', help='Remove hashes with invalid paths from benchmark table',
                             action='store_true')
    parser_init.set_defaults(func=cli_init)

    parser_hash = subparsers.add_parser('hash', help='Print hash for a single file')
    parser_hash.add_argument('path', type=file_type, help="Path to one benchmark")
    parser_hash.set_defaults(func=cli_hash)

    parser_import = subparsers.add_parser('import', help='Import attributes from comma-separated csv-file with header')
    parser_import.add_argument('path', type=file_type, help="Path to csv-file")
    parser_import.add_argument('-k', '--key', type=column_type,
                               help="Name of the key column (the hash-value of the problem)", required=True)
    parser_import.add_argument('-s', '--source', type=column_type, help="Source name of column to import (in csv-file)",
                               required=True)
    parser_import.add_argument('-t', '--target', type=column_type, help="Target name of column to import (in database)",
                               required=True)
    parser_import.set_defaults(func=cli_import)

    # define reflection
    parser_reflect = subparsers.add_parser('reflect', help='Reflection, Display Groups')
    parser_reflect.add_argument('name', type=column_type, help='Display Details on Group', nargs='?')
    parser_reflect.add_argument('-v', '--values', action='store_true', help='Display Distinct Values of Group')
    parser_reflect.set_defaults(func=cli_reflection)

    # define create command sub-structure
    parser_group = subparsers.add_parser('group', help='Create or modify an attribute group')
    parser_group.add_argument('name', type=column_type, help='Name of group to create (or modify)')
    parser_group.add_argument('-u', '--unique', help='Specify if the group stores unique or '
                              '(by default) several attributes per benchmark (expects default value which has to match '
                              'type if set)')
    parser_group.add_argument('-t', '--type', help='Specify the value type of the group (default: text)',
                              default="text", choices=['text', 'integer', 'real'])
    parser_group.add_argument('-r', '--remove', action='store_true',
                              help='If group exists: remove the group with the specified name')
    parser_group.add_argument('-c', '--clear', action='store_true',
                              help='If group exists: remove all values in the group with the specified name')
    parser_group.set_defaults(func=cli_group)

    # define set command sub-structure
    parser_tag = subparsers.add_parser('tag',
                                       help='Associate attribues with benchmarks (hashes read line-wise from stdin)')
    parser_tag.add_argument('name', type=column_type, help='Name of attribute group')
    parser_tag.add_argument('-v', '--value', help='Attribute value', required=True)
    parser_tag.add_argument('-r', '--remove', action='store_true',
                            help='Remove attribute from hashes if present, instead of adding it')
    parser_tag.add_argument('-f', '--force', action='store_true', help='Overwrite existing values')
    parser_tag.set_defaults(func=cli_tag)

    # define find command sub-structure
    parser_query = subparsers.add_parser('query', help='Query the benchmark database')
    parser_query.add_argument('query', help='Specify a query-string (e.g. "variables > 100 and path like %%mp1%%")',
                              nargs='?')
    parser_query.add_argument('-u', '--union', help='Read hashes from stdin and create union with query results',
                              action='store_true')
    parser_query.add_argument('-i', '--intersection',
                              help='Read hashes from stdin and create intersection with query results',
                              action='store_true')
    parser_query.set_defaults(func=cli_query)

    # define resolve command
    parser_resolve = subparsers.add_parser('resolve', help='Resolve Hashes')
    parser_resolve.add_argument('name', type=column_type, help='Name of group to resolve against',
                                default=["benchmarks"], nargs='*')
    parser_resolve.add_argument('-c', '--collapse', action='store_true', help='Show only one representative per hash')
    parser_resolve.add_argument('-p', '--pattern', help='Substring that must occur in path')
    parser_resolve.set_defaults(func=cli_resolve)

    # evaluate arguments
    if (len(sys.argv) > 1):
        args = parser.parse_args()
        args.func(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
