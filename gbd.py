#!/usr/bin/python3
# -*- coding: utf-8 -*-

# GBD Benchmark Database (GBD)
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

import argparse
import os
import re
from os.path import join, dirname, realpath
import sys
from gbd_tool.util import eprint, confirm, read_hashes
from gbd_tool.gbd_api import GbdApi
from gbd_tool.db import Database
from gbd_tool import benchmark_administration

def cli_hash(api: GbdApi, args):
    path = os.path.abspath(args.path)
    print(GbdApi.hash_file(path))

def cli_import(api: GbdApi, args):
    path = os.path.abspath(args.path)
    api.import_file(path, args.key, args.source, args.target)

def cli_init(api: GbdApi, args):
    path = os.path.abspath(args.path)
    api.init_database(path)

def cli_bootstrap(api: GbdApi, args):
    api.bootstrap(args.algo)

def cli_sanitize(api: GbdApi, args):
    api.sanitize(args.hashes)

# create feature
def cli_create(api: GbdApi, args):
    if not api.feature_exists(args.name):
        api.create_feature(args.name, args.unique)
    else:
        eprint("Feature '{}' does already exist".format(args.name))

# delete feature
def cli_delete(api: GbdApi, args):
    if api.feature_exists(args.name):
        if (not args.hashes or len(args.hashes) == 0) and not sys.stdin.isatty():
            args.hashes = read_hashes()
        if args.hashes and len(args.hashes) > 0:
            if args.force or confirm("Delete attributes of given hashes from '{}'?".format(args.name)):
                api.remove_attributes(args.name, args.hashes)
        elif args.force or confirm("Delete feature '{}' and all associated attributes?".format(args.name)):
            api.remove_feature(args.name)
    else:
        eprint("Feature '{}' does not exist".format(args.name))

# entry for query command
def cli_get(api: GbdApi, args):
    if (not args.query or len(args.query) == 0) and not sys.stdin.isatty():
        hashes = read_hashes()
        resultset = api.hash_search(hashes, args.resolve, args.collapse, args.group_by)
    else:
        resultset = api.query_search(args.query, args.resolve, args.collapse, args.group_by)
    for result in resultset:
        print(args.separator.join([(str(item or '')) for item in result]))


# associate an attribute with a hash and a value
def cli_set(api: GbdApi, args):
    if (not args.hashes or len(args.hashes) == 0) and not sys.stdin.isatty():
        args.hashes = read_hashes()
    api.set_attribute(args.name, args.value, args.hashes, args.force)


def cli_par2(api: GbdApi, args):
    api.calculate_par2_score(args.query, args.name)


def cli_meta_get(api: GbdApi, args):
    info = api.meta_get(args.feature)
    print(info)

def cli_meta_set(api: GbdApi, args):
    api.meta_set(args.feature, args.name, args.value)

def cli_meta_clear(api: GbdApi, args):
    api.meta_clear(args.feature, args.name)


def cli_info(api: GbdApi, args):
    if args.name is not None:
        info = api.get_feature_info(args.name)
        for k,v in info.items():
            print("{}: {}".format(k, v))
    else:
        print("Features: {}".format(" ".join(api.get_material_features())))
        print("Virtual: {}".format(" ".join(api.get_virtual_features())))


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
    parser.add_argument('-j', "--jobs", help='Specify number of parallel jobs', default=1, nargs='?')

    parser.add_argument('-s', "--separator", choices=[" ", ",", ";"], default=" ", help="Feature separator (outer delimiter used in import and output)")
    parser.add_argument('-i', "--inner-separator", choices=[" ", ",", ";"], default=",", help="Inner separator (used to group multiple values in one column)")
    
    parser.add_argument('-t', "--join-type", choices=["INNER", "OUTER", "LEFT"], default="INNER", help="Join Type: treatment of missing values in queries")

    subparsers = parser.add_subparsers(help='Available Commands:')

    parser_init = subparsers.add_parser('init', help='Initialize Database')
    parser_init.add_argument('path', type=directory_type, help="Path to benchmarks")
    parser_init.set_defaults(func=cli_init)

    # define bootstrap command sub-structure
    parser_algo = subparsers.add_parser('bootstrap', help='Calculate hard-coded sets of instance attributes')
    parser_algo.add_argument('algo', help='Specify which attributes to bootstrap', nargs='?', default='clause_types', choices=['clause_types', 'degree_sequence_hash'])
    parser_algo.set_defaults(func=cli_bootstrap)

    parser_sanitize = subparsers.add_parser('sanitize', help='Print sanitation info for given hashes')
    parser_sanitize.add_argument('hashes', help='Hashes', nargs='+')
    parser_sanitize.set_defaults(func=cli_sanitize)

    parser_import = subparsers.add_parser('import', help='Import attributes from csv-file')
    parser_import.add_argument('path', type=file_type, help="Path to csv-file")
    parser_import.add_argument('-k', '--key', type=column_type, help="Name of the key column (gbd-hash)", required=True)
    parser_import.add_argument('-s', '--source', help="Name of source column in csv-file", required=True)
    parser_import.add_argument('-t', '--target', type=column_type, help="Name of target column (in database)", required=True)
    parser_import.set_defaults(func=cli_import)

    # define info
    parser_reflect = subparsers.add_parser('info', help='Print info about available features')
    parser_reflect.add_argument('name', type=column_type, help='Print info about specified feature', nargs='?')
    parser_reflect.set_defaults(func=cli_info)

    parser_hash = subparsers.add_parser('hash', help='Print hash for a single file')
    parser_hash.add_argument('path', type=file_type, help="Path to one benchmark")
    parser_hash.set_defaults(func=cli_hash)

    # define create command sub-structure
    parser_create = subparsers.add_parser('create', help='Create a new feature')
    parser_create.add_argument('name', type=column_type, help='Name of feature')
    parser_create.add_argument('-u', '--unique', help='Unique constraint: specify default-value of feature')
    parser_create.set_defaults(func=cli_create)

    parser_delete = subparsers.add_parser('delete', help='Delete all values assiociated with given hashes or remove feature if no hashes are given')
    parser_delete.add_argument('hashes', help='Hashes', nargs='*')
    parser_delete.add_argument('name', type=column_type, help='Name of feature')
    parser_delete.add_argument('-f', '--force', action='store_true', help='Do not ask for confirmation')
    parser_delete.set_defaults(func=cli_delete)

    # define set command sub-structure
    parser_set = subparsers.add_parser('set', help='Set attribute [name] to [value] for [hashes]')
    parser_set.add_argument('hashes', help='Hashes', nargs='*')
    parser_set.add_argument('-n', '--name', type=column_type, help='Attribute name', required=True)
    parser_set.add_argument('-v', '--value', help='Attribute value', required=True)
    parser_set.add_argument('-f', '--force', action='store_true', help='Overwrite existing unique values')
    parser_set.set_defaults(func=cli_set)

    # define get command sub-structure
    parser_get = subparsers.add_parser('get', help='Query the benchmark database')
    parser_get.add_argument('query', help='Specify a query-string (e.g. "variables > 100 and path like %%mp1%%")', nargs='?')
    parser_get.add_argument('-r', '--resolve', help='Names of groups to resolve hashes against', nargs='+')
    parser_get.add_argument('-c', '--collapse', action='store_true', help='Show only one representative per hash')
    parser_get.add_argument('-g', '--group_by', help='Group by specified attribute (instead of gbd-hash)')
    parser_get.set_defaults(func=cli_get)

    # meta-features
    parser_meta = subparsers.add_parser('meta', help='Control meta-features')
    parser_meta_subparsers = parser_meta.add_subparsers(help='Sub-Commands')
    parser_meta_get = parser_meta_subparsers.add_parser('get', help='Get feature meta-info')
    parser_meta_get.add_argument('feature', type=column_type, help='Specify feature')
    parser_meta_get.set_defaults(func=cli_meta_get)

    parser_meta_set = parser_meta_subparsers.add_parser('set', help='Set feature meta-info')
    parser_meta_set.add_argument('feature', type=column_type, help='Specify feature')
    parser_meta_set.add_argument('-n', '--name', type=column_type, help='Meta-feature name', required=True)
    parser_meta_set.add_argument('-v', '--value', help='Meta-feature value', required=True)
    parser_meta_set.set_defaults(func=cli_meta_set)

    parser_meta_clear = parser_meta_subparsers.add_parser('clear', help='Clear feature meta-info')
    parser_meta_clear.add_argument('feature', type=column_type, help='Specify feature')
    parser_meta_clear.add_argument('-n', '--name', type=column_type, help='Meta-feature name')
    parser_meta_clear.set_defaults(func=cli_meta_clear)

    # par2 score
    parser_par2 = subparsers.add_parser('par2', help='Calculate PAR-2 score for given runtime feature')
    parser_par2.add_argument('query', help='Specify a query-string (e.g. "variables > 100 and path like %%mp1%%")', nargs='?')
    parser_par2.add_argument('name', type=column_type, help='Name of runtime feature')
    parser_par2.set_defaults(func=cli_par2)

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
        eprint("Database: {}".format(args.db))
        try:
            with GbdApi(args.db, int(args.jobs), args.separator, args.inner_separator, args.join_type) as api:
                args.func(api, args)
        except AttributeError as e:
            eprint(e)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
