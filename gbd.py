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
from itertools import combinations
from operator import itemgetter

from gbd_tool.util import eprint, confirm, read_hashes, is_number
from gbd_tool.gbd_api import GbdApi
from gbd_tool.error import *

def cli_hash(api: GbdApi, args):
    path = os.path.abspath(args.path)
    print(GbdApi.hash_file(path))

def cli_import(api: GbdApi, args):
    path = os.path.abspath(args.path)
    api.import_file(path, args.key, args.source, args.target)

def cli_init_local(api: GbdApi, args):
    path = os.path.abspath(args.path)
    api.init_database(path)

def cli_init_ct(api: GbdApi, args):
    api.bootstrap("clause_types", args.hashes)

def cli_init_dsh(api: GbdApi, args):
    api.bootstrap("degree_sequence_hash", args.hashes)

def cli_init_sanitize(api: GbdApi, args):
    api.bootstrap("sanitation_info", args.hashes)

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
        eprint("Feature '{}' does not exist or is virtual".format(args.name))

# rename feature
def cli_rename(api: GbdApi, args):
    if not api.feature_exists(args.old_name):
        eprint("Feature '{}' does not exist or is virtual".format(args.old_name))
    elif api.feature_exists(args.new_name):
        eprint("Feature '{}' does already exist".format(args.new_name))
    else:
        api.rename_feature(args.old_name, args.new_name)
        
# entry for query command
def cli_get(api: GbdApi, args):
    hashes = []
    if not sys.stdin.isatty():
        hashes = read_hashes()
    resultset = api.query_search(args.query, hashes, args.resolve, args.collapse, args.group_by)
    for result in resultset:
        print(args.separator.join([(str(item or '')) for item in result]))


# associate an attribute with a hash and a value
def cli_set(api: GbdApi, args):
    if (not args.hashes or len(args.hashes) == 0) and not sys.stdin.isatty():
        args.hashes = read_hashes()
    api.set_attribute(args.name, args.value, args.hashes, args.force)
    

def cli_info_set(api: GbdApi, args):
    api.meta_set(args.feature, args.name, args.value)

def cli_info_clear(api: GbdApi, args):
    api.meta_clear(args.feature, args.name)

def cli_info(api: GbdApi, args):
    if args.name is None:
        for db_str in api.get_databases():
            print("Database: {}".format(db_str))
            print("Features: {}".format(" ".join(api.get_material_features(db_str))))
            print("Virtual: {}".format(" ".join(api.get_virtual_features(db_str))))
    else:
        info = api.get_feature_info(args.name)
        for key in info:
            print("{}: {}".format(key, info[key]))


def cli_eval_par2(api: GbdApi, args):
    for name in args.runtimes:
        par2 = api.calculate_par2_score(args.query, name, args.timeout)
        print(name + ": " + str(par2))
    if args.vbs:
        vbs_par2 = api.calculate_vbs_par2(args.query, args.runtimes, args.timeout)
        print("vbs: " + str(vbs_par2))

def cli_eval_vbs(api: GbdApi, args):
    resultset = api.calculate_vbs(args.query, args.runtimes, args.timeout)
    for result in resultset:
        print(args.separator.join([(str(item or '')) for item in result]))

def cli_eval_combinations(api: GbdApi, args):
    result = api.query_search(args.query, [], args.runtimes)
    result = [[float(val) if is_number(val) and float(val) < float(args.timeout) else 2*args.timeout for val in row] for row in result]
    args.runtimes.insert(0, "dummy")
    for comb in combinations(range(1, len(args.runtimes)), args.size):
        comb_par2 = sum([min(itemgetter(*comb)(row)) for row in result]) / len(result)
        print(str(itemgetter(*comb)(args.runtimes)) + ": " + str(comb_par2))


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
    parser.add_argument('-s', "--separator", choices=[" ", ",", ";"], default=" ", help="Feature separator (delimiter used in import and output)")
    parser.add_argument('-t', "--join-type", choices=["INNER", "OUTER", "LEFT"], default="LEFT", help="Join Type: treatment of missing values in queries")
    parser.add_argument('-v', '--verbose', action='store_true', help='Print additional (or diagnostic) information to stderr')

    subparsers = parser.add_subparsers(help='Available Commands:')

    # INITIALIZATION AND BOOTSTRAPPING
    parser_init = subparsers.add_parser('init', help='Initialize Database')
    parser_init_subparsers = parser_init.add_subparsers(help='Select Initialization Procedure:')
    # init local paths:
    parser_init_local = parser_init_subparsers.add_parser('local', help='Initialize Local Hash/Path Entries')
    parser_init_local.add_argument('path', type=directory_type, help="Path to benchmarks")
    parser_init_local.set_defaults(func=cli_init_local)
    # init clause types:
    parser_init_ct = parser_init_subparsers.add_parser('clause_types', help='Initialize Clause-Type Tables')
    parser_init_ct.add_argument('hashes', help='Hashes', nargs='+')
    parser_init_ct.set_defaults(func=cli_init_ct)
    # init degree_sequence_hash:
    parser_init_dsh = parser_init_subparsers.add_parser('degree_sequence_hash', help='Initialize Degree-Sequence Hash')
    parser_init_dsh.add_argument('hashes', help='Hashes', nargs='+')
    parser_init_dsh.set_defaults(func=cli_init_dsh)
    # init sanitation info
    parser_init_sanitize = parser_init_subparsers.add_parser('sanitize', help='Check Instances, Store Sanitation Info')
    parser_init_sanitize.add_argument('hashes', help='Hashes', nargs='+')
    parser_init_sanitize.set_defaults(func=cli_init_sanitize)

    # GBD HASH
    parser_hash = subparsers.add_parser('hash', help='Print hash for a single file')
    parser_hash.add_argument('path', type=file_type, help="Path to one benchmark")
    parser_hash.set_defaults(func=cli_hash)

    # GET/SET ATTRIBUTES
    parser_get = subparsers.add_parser('get', help='Get data by query (or hash-list via stdin)')
    parser_get.add_argument('query', help='Specify a query-string (e.g. "variables > 100 and path like %%mp1%%")', nargs='?')
    parser_get.add_argument('-r', '--resolve', help='List of features to resolve against', nargs='+')
    parser_get.add_argument('-c', '--collapse', default='group_concat', 
                            choices=['group_concat', 'min', 'max', 'avg', 'count', 'sum'], 
                            help='Treatment of multiple values per hash (or grouping value resp.)')
    parser_get.add_argument('-g', '--group_by', default='hash', help='Group by specified attribute value')
    parser_get.set_defaults(func=cli_get)

    parser_set = subparsers.add_parser('set', help='Set specified attribute-value for given hashes (via argument or stdin)')
    parser_set.add_argument('hashes', help='Hashes', nargs='*')
    parser_set.add_argument('-n', '--name', type=column_type, help='Feature name', required=True)
    parser_set.add_argument('-v', '--value', help='Attribute value', required=True)
    parser_set.add_argument('-f', '--force', action='store_true', help='Overwrite existing unique values')
    parser_set.set_defaults(func=cli_set)

    # IMPORT DATA FROM CSV
    parser_import = subparsers.add_parser('import', help='Import attributes from csv-file')
    parser_import.add_argument('path', type=file_type, help="Path to csv-file")
    parser_import.add_argument('-k', '--key', type=column_type, help="Name of the key column (gbd-hash)", required=True)
    parser_import.add_argument('-s', '--source', help="Name of source column in csv-file", required=True)
    parser_import.add_argument('-t', '--target', type=column_type, help="Name of target column (in database)", required=True)
    parser_import.set_defaults(func=cli_import)

    # CREATE/DELETE/MODIFY FEATURES
    parser_create = subparsers.add_parser('create', help='Create a new feature')
    parser_create.add_argument('name', type=column_type, help='Name of feature')
    parser_create.add_argument('-u', '--unique', help='Unique constraint: specify default-value of feature')
    parser_create.set_defaults(func=cli_create)

    parser_delete = subparsers.add_parser('delete', help='Delete all values assiociated with given hashes (via argument or stdin) or remove feature if no hashes are given')
    parser_delete.add_argument('hashes', help='Hashes', nargs='*')
    parser_delete.add_argument('name', type=column_type, help='Name of feature')
    parser_delete.add_argument('-f', '--force', action='store_true', help='Do not ask for confirmation')
    parser_delete.set_defaults(func=cli_delete)

    parser_rename = subparsers.add_parser('rename', help='Rename feature')
    parser_rename.add_argument('old_name', type=column_type, help='Old name of feature')
    parser_rename.add_argument('new_name', type=column_type, help='New name of feature')
    parser_rename.set_defaults(func=cli_rename)

    # HANDLE META-FEATURES (e.g. specify runtime meta-data like timeout/memout/machine)
    parser_info = subparsers.add_parser('info', help='Print info about available features')
    parser_info.add_argument('name', type=column_type, help='Print info about specified feature', nargs='?')
    parser_info.set_defaults(func=cli_info)

    parser_info_set = subparsers.add_parser('info_set', help='Set feature meta-attributes')
    parser_info_set.add_argument('feature', type=column_type, help='Feature name')
    parser_info_set.add_argument('-n', '--name', type=column_type, help='Meta-feature name', required=True)
    parser_info_set.add_argument('-v', '--value', help='Meta-feature value', required=True)
    parser_info_set.set_defaults(func=cli_info_set)

    parser_info_clear = subparsers.add_parser('info_clear', help='Clear feature meta-attributes')
    parser_info_clear.add_argument('feature', type=column_type, help='Feature name')
    parser_info_clear.add_argument('-n', '--name', type=column_type, help='Meta-feature name')
    parser_info_clear.set_defaults(func=cli_info_clear)

    # SCORE CALCULATION
    parser_eval = subparsers.add_parser('eval', help='Evaluate Runtime Features')
    parser_eval_subparsers = parser_eval.add_subparsers(help='Select Evaluation Procedure')

    parser_eval_par2 = parser_eval_subparsers.add_parser('par2', help='Calculate PAR-2 Score')
    parser_eval_par2.add_argument('query', help='Specify a GBD Query', nargs='?')
    parser_eval_par2.add_argument('-r', '--runtimes', help='List of runtime features', nargs='+')
    parser_eval_par2.add_argument('-t', '--timeout', default=5000, type=int, help='Name of runtime feature')
    parser_eval_par2.add_argument('--vbs', action='store_true', help='Also Calculate PAR-2 Score of VBS')
    parser_eval_par2.set_defaults(func=cli_eval_par2)

    parser_eval_vbs = parser_eval_subparsers.add_parser('vbs', help='Calculate VBS')
    parser_eval_vbs.add_argument('query', help='Specify a GBD Query', nargs='?')
    parser_eval_vbs.add_argument('-r', '--runtimes', help='List of runtime features', nargs='+')
    parser_eval_vbs.add_argument('-t', '--timeout', default=5000, type=int, help='Name of runtime feature')
    parser_eval_vbs.set_defaults(func=cli_eval_vbs)

    parser_eval_comb = parser_eval_subparsers.add_parser('comb', help='Calculate VBS of Solver Combinations')
    parser_eval_comb.add_argument('query', help='Specify a GBD Query', nargs='?')
    parser_eval_comb.add_argument('-k', '--size', default=2, type=int, help='Number of Solvers per Combination')
    parser_eval_comb.add_argument('-r', '--runtimes', help='List of runtime features', nargs='+')
    parser_eval_comb.add_argument('-t', '--timeout', default=5000, type=int, help='Name of runtime feature')
    parser_eval_comb.set_defaults(func=cli_eval_combinations)

    # EVALUATE ARGUMENTS
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
            with GbdApi(args.db, int(args.jobs), args.separator, args.join_type, args.verbose) as api:
                args.func(api, args)
        except GbdApiError as err:
            eprint(err)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
