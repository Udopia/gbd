#!/usr/bin/python3
# -*- coding: utf-8 -*-
import argparse
import os
import re
import sys
import gzip
import bz2
from os.path import join, dirname, realpath

from main.gbd_tool.gbd_api import GbdApi
from gbd_tool.util import eprint, confirm

from server.interface import SERVER_CONFIG_PATH

config_path = join(dirname(realpath(__file__)), "cli_config")


def cli_hash(args):
    path = os.path.abspath(args.path)
    eprint('Hashing Benchmark: {}'.format(path))
    print(GbdApi.hash_file(path))


def cli_import(args):
    path = os.path.abspath(args.path)
    eprint('Importing Data from CSV-File: {}'.format(path))
    api = GbdApi(config_path, args.db)
    api.import_file(path, args.key, args.source, args.target, args.delimiter)


def cli_init(args):
    path = os.path.abspath(args.path)
    api = GbdApi(config_path, args.db)
    if args.path is not None:
        eprint('Removing invalid benchmarks from path: {}'.format(path))
        eprint('Registering benchmarks from path: {}'.format(path))
        api.init_database(path)
    else:
        api.init_database()


def cli_algo(args):
    api = GbdApi(config_path, args.db)
    api.add_attribute_group("clauses_positive", "integer", 0)
    api.add_attribute_group("clauses_negative", "integer", 0)
    api.add_attribute_group("clauses_horn", "integer", 0)

    resultset = api.query_search("(clauses_horn = 0) and (clauses_positive = 0) and (clauses_negative = 0)", ["benchmarks"])
    for result in resultset:
        c_horn = 0
        c_pos = 0
        c_neg = 0
        hashvalue = result[0].split(',')[0]
        path = result[1].split(',')[0]
        eprint(hashvalue)
        eprint(path)
        cnffile = None
        if path.endswith('.cnf.gz'):
            cnffile = gzip.open(path, 'rt')
        elif path.endswith('.cnf.bz2'):
            cnffile = bz2.open(path, 'rt')
        else:
            cnffile = open(path, 'rt')
        
        eprint("Parsing {}".format(path))
        for line in cnffile:
            if line.strip() and len(line.strip().split()) > 1:
                parts = line.strip().split()[:-1]
                if parts[0][0] == 'c' or parts[0][0] == 'p':
                    continue
                n_neg = sum(int(part) < 0 for part in parts)
                if n_neg < 2:
                    c_horn += 1
                    if n_neg == 0:
                        c_pos += 1
                elif n_neg == len(parts):
                    c_neg += 1
        api.set_attribute("clauses_positive", c_pos, [ hashvalue ], True)
        api.set_attribute("clauses_negative", c_neg, [ hashvalue ], True)
        api.set_attribute("clauses_horn", c_horn, [ hashvalue ], True)
        cnffile.close()


# entry for modify command
def cli_group(args):
    if args.name.startswith("__"):
        eprint("Names starting with '__' are reserved for system tables")
        return
    api = GbdApi(config_path, args.db)
    if api.check_group_exists(args.name):
        eprint("Group {} does already exist".format(args.name))
    elif not args.remove and not args.clear:
        eprint("Adding or modifying group '{}', unique {}, type {}, default-value {}".format(
            args.name, args.unique is not None, args.type, args.unique))
        api.add_attribute_group(args.name, args.type, args.unique)
        return
    if not api.check_group_exists(args.name):
        eprint("Group '{}' does not exist".format(args.name))
        return
    if args.remove and confirm("Delete group '{}'?".format(args.name)):
        api.remove_attribute_group(args.name)
    else:
        if args.clear and confirm("Clear group '{}'?".format(args.name)):
            api.clear_group(args.name)
    return


# entry for query command
def cli_get(args):
    eprint("Querying {} ...".format(args.db))
    try:
        api = GbdApi(config_path, args.db)
        resultset = api.query_search(args.query, args.resolve)
    except ValueError as e:
        eprint(e)
        return
    if args.collapse:
        for result in resultset:
            print(" ".join([item.split(',')[0] for item in result]))
    else:
        for result in resultset:
            print(" ".join([(item or '') for item in result]))
    return


# associate an attribute with a hash and a value
def cli_set(args):
    api = GbdApi(config_path, args.db)
    if args.remove and (args.force or confirm("Delete tag '{}' from '{}'?".format(args.value, args.name))):
        api.remove_attribute(args.name, args.value, args.hashes)
    else:
        api.set_attribute(args.name, args.value, args.hashes, args.force)


def cli_info(args):
    api = GbdApi(config_path, args.db)
    if args.name is not None:
        if args.values:
            info = api.get_group_values(args.name)
            print(*info, sep='\n')
        else:
            info = api.get_group_info(args.name)
            print('name: {}'.format(info.get('name')))
            print('type: {}'.format(info.get('type')))
            print('uniqueness: {}'.format(info.get('uniqueness')))
            print('default value: {}'.format(info.get('default')))
            print('number of entries: {}'.format(*info.get('entries')))
    else:
        result = api.get_database_info()
        print("DB '{}' was created with version: {} and HASH version: {}".format(result.get('name'),
                                                                                 result.get('version'),
                                                                                 result.get('hash-version')))
        print("Found tables:")
        print(*api.get_all_groups())


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

    parser.add_argument('-d', "--db", help='Specify database to work with', default=os.environ.get('GBD_DB'), nargs='?')

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
    parser_import.add_argument('-s', '--source', help="Source name of column to import (in csv-file)",
                               required=True)
    parser_import.add_argument('-t', '--target', type=column_type, help="Target name of column to import (in Database)",
                               required=True)
    parser_import.add_argument('-d', '--delimiter', choices=[" ", ",", ";"], default=" ", help="Delimiter")
    parser_import.set_defaults(func=cli_import)

    # define info
    parser_reflect = subparsers.add_parser('info', help='Get information, Display Groups')
    parser_reflect.add_argument('name', type=column_type, help='Display Details on Group, info of Database if none',
                                nargs='?')
    parser_reflect.add_argument('-v', '--values', action='store_true', help='Display Distinct Values of Group if given')
    parser_reflect.set_defaults(func=cli_info)

    # define create command sub-structure
    parser_algo = subparsers.add_parser('algo', help='Execute a named algorithm on the given benchmark and store all the data')
    #parser_algo.add_argument('path', type=file_type, help="Path to one benchmark")
    parser_algo.set_defaults(func=cli_algo)

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
    parser_tag = subparsers.add_parser('set', help='Sets the value of attribute [name] to [value] for benchmark [hash]')
    parser_tag.add_argument('hashes', help='Hashes', nargs='+')
    parser_tag.add_argument('-n', '--name', type=column_type, help='Attribute name', required=True)
    parser_tag.add_argument('-v', '--value', help='Attribute value', required=True)
    parser_tag.add_argument('-r', '--remove', action='store_true',
                            help='Remove attribute from hashes if present, instead of adding it')
    parser_tag.add_argument('-f', '--force', action='store_true', help='Overwrite existing values')
    parser_tag.set_defaults(func=cli_set)

    # define find command sub-structure
    parser_query = subparsers.add_parser('get', help='Query the benchmark database')
    parser_query.add_argument('query', help='Specify a query-string (e.g. "variables > 100 and path like %%mp1%%")', 
                              nargs='?')
    parser_query.add_argument('-r', '--resolve', help='Names of groups to resolve hashes against', nargs='+')
    parser_query.add_argument('-c', '--collapse', action='store_true', help='Show only one representative per hash')
    parser_query.set_defaults(func=cli_get)

    # evaluate arguments
    if len(sys.argv) > 1:
        args = parser.parse_args()
        args.func(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
