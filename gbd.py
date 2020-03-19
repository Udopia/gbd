#!/usr/bin/python3
# -*- coding: utf-8 -*-

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

import argparse
import os
import re
from os.path import join, dirname, realpath
import sys
from gbd_tool.util import eprint, confirm
from gbd_tool.gbd_api import GbdApi
from gbd_tool.db import Database
from gbd_tool import benchmark_administration

def cli_hash(args):
    path = os.path.abspath(args.path)
    print(GbdApi.hash_file(path))

def cli_import(args):
    path = os.path.abspath(args.path)
    eprint('Importing Data from CSV-File: {}'.format(path))
    api = GbdApi(args.db)
    api.import_file(path, args.key, args.source, args.target, args.delimiter)


def cli_init(args):
    path = os.path.abspath(args.path)
    api = GbdApi(args.db)
    api.init_database(path, int(args.jobs))

def cli_bootstrap(args):
    api = GbdApi(args.db)
    api.bootstrap(args.algo, int(args.jobs))

# entry for modify command
def cli_group(args):
    if args.name.startswith("__"):
        eprint("Names starting with '__' are reserved for system tables")
        return
    api = GbdApi(args.db)
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


# entry for query command
def cli_get(args):
    eprint("Querying {} ...".format(args.db))
    try:
        api = GbdApi(args.db)
        eprint(str(args))
        resultset = api.query_search(args.query, args.resolve, args.collapse, args.group_by)
    except ValueError as e:
        eprint(e)
        return
    for result in resultset:
        print(" ".join([(str(item or '')) for item in result]))


# associate an attribute with a hash and a value
def cli_set(args):
    api = GbdApi(args.db)
    if args.remove and (args.force or confirm("Delete tag '{}' from '{}'?".format(args.value, args.name))):
        api.remove_attribute(args.name, args.value, args.hashes)
    else:
        api.set_attribute(args.name, args.value, args.hashes, args.force)


def cli_info(args):
    api = GbdApi(args.db)
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
        print("Using '{}'".format(result.get('name')))
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
    parser.add_argument('-j', "--jobs", help='Specify number of jobs used by init or algo', default=1, nargs='?')

    subparsers = parser.add_subparsers(help='Available Commands:')

    parser_init = subparsers.add_parser('init', help='Initialize Database')
    parser_init.add_argument('path', type=directory_type, help="Path to benchmarks")
    parser_init.set_defaults(func=cli_init)

    # define create command sub-structure
    parser_algo = subparsers.add_parser('bootstrap', help='Calculate hard-coded sets of instance attributes')
    parser_algo.add_argument('algo', help='Specify which attributes to bootstrap', nargs='?', default='clause_types', choices=['clause_types', 'sorted_hash'])
    parser_algo.set_defaults(func=cli_bootstrap)

    parser_import = subparsers.add_parser('import', help='Import attributes from csv-file')
    parser_import.add_argument('path', type=file_type, help="Path to csv-file")
    parser_import.add_argument('-k', '--key', type=column_type, help="Name of the key column (the hash-value of the problem)", required=True)
    parser_import.add_argument('-s', '--source', help="Source name of column to import (in csv-file)", required=True)
    parser_import.add_argument('-t', '--target', type=column_type, help="Target name of column to import (in Database)", required=True)
    parser_import.add_argument('-d', '--delimiter', choices=[" ", ",", ";"], default=" ", help="Delimiter")
    parser_import.set_defaults(func=cli_import)

    # define info
    parser_reflect = subparsers.add_parser('info', help='Get information, Display Groups')
    parser_reflect.add_argument('name', type=column_type, help='Display Details on Group, info of Database if none', nargs='?')
    parser_reflect.add_argument('-v', '--values', action='store_true', help='Display Distinct Values of Group if given')
    parser_reflect.set_defaults(func=cli_info)

    parser_hash = subparsers.add_parser('hash', help='Print hash for a single file')
    parser_hash.add_argument('path', type=file_type, help="Path to one benchmark")
    parser_hash.set_defaults(func=cli_hash)

    # define create command sub-structure
    parser_group = subparsers.add_parser('group', help='Create or modify an attribute group')
    parser_group.add_argument('name', type=column_type, help='Name of group to create (or modify)')
    parser_group.add_argument('-u', '--unique', help='Attribute has one unique value per benchmark (expects a default value)')
    parser_group.add_argument('-t', '--type', help='Specify the value type of the group (default: text)', default="text", choices=['text', 'integer', 'real'])
    parser_group.add_argument('-r', '--remove', action='store_true', help='If group exists: remove the group with the specified name')
    parser_group.add_argument('-c', '--clear', action='store_true', help='If group exists: remove all values in the group with the specified name')
    parser_group.set_defaults(func=cli_group)

    # define set command sub-structure
    parser_tag = subparsers.add_parser('set', help='Set attribute [name] to [value] for [hashes]')
    parser_tag.add_argument('hashes', help='Hashes', nargs='+')
    parser_tag.add_argument('-n', '--name', type=column_type, help='Attribute name', required=True)
    parser_tag.add_argument('-v', '--value', help='Attribute value', required=True)
    parser_tag.add_argument('-r', '--remove', action='store_true', help='Remove attribute from hashes if present, instead of adding it')
    parser_tag.add_argument('-f', '--force', action='store_true', help='Overwrite existing values')
    parser_tag.set_defaults(func=cli_set)

    # define find command sub-structure
    parser_query = subparsers.add_parser('get', help='Query the benchmark database')
    parser_query.add_argument('query', help='Specify a query-string (e.g. "variables > 100 and path like %%mp1%%")', nargs='?')
    parser_query.add_argument('-r', '--resolve', help='Names of groups to resolve hashes against', nargs='+')
    parser_query.add_argument('-c', '--collapse', action='store_true', help='Show only one representative per hash')
    parser_query.add_argument('-g', '--group_by', help='Group by specified attribute (instead of gbd-hash)')
    parser_query.set_defaults(func=cli_get)

    # evaluate arguments
    args = parser.parse_args()
    if not args.db:
            eprint("""No database path is given. 
A database path can be given in two ways:
-- by setting the environment variable GBD_DB
-- by giving a path via --db=[path]
A database file containing some attributes of instances used in the SAT Competitions can be obtained at http://gbd.iti.kit.edu/getdatabase
Initialize your database with local paths to your benchmark instances by using the init-command. """)
    elif len(sys.argv) > 1:
        try:
            args.func(args)
        except AttributeError as e:
            eprint(e)
            parser.print_help()
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
