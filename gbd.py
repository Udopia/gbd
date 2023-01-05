#!/usr/bin/python3

# MIT License

# Copyright (c) 2023 Markus Iser, Karlsruhe Institute of Technology (KIT)

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

import os
import sys
import traceback

from gbd_core.api import GBD, GBDException
from gbd_core.grammar import ParserException
from gbd_core import util, contexts, schema
from gbd_core.util_argparse import *


### Command-Line Interface Entry Points
def cli_hash(api: GBD, args):
    from gbd_init.gbdhash import gbd_hash
    print(gbd_hash(args.path))


def cli_init_local(api: GBD, args):
    from gbd_init.cnf_extractors import init_local
    rlimits = { 'jobs': args.jobs, 'tlim': args.tlim, 'mlim': args.mlim, 'flim': args.flim }
    init_local(api, args.context, rlimits, args.path, target_db=args.target_db)

def cli_init_base_features(api: GBD, args):
    from gbd_init.cnf_extractors import init_base_features
    rlimits = { 'jobs': args.jobs, 'tlim': args.tlim, 'mlim': args.mlim, 'flim': args.flim }
    init_base_features(api, args.context, rlimits, args.query, args.hashes, target_db=args.target_db)

def cli_init_gate_features(api: GBD, args):
    from gbd_init.cnf_extractors import init_gate_features
    rlimits = { 'jobs': args.jobs, 'tlim': args.tlim, 'mlim': args.mlim, 'flim': args.flim }
    init_gate_features(api, args.context, rlimits, args.query, args.hashes, target_db=args.target_db)

def cli_init_iso(api: GBD, args):
    from gbd_init.cnf_extractors import init_isohash
    rlimits = { 'jobs': args.jobs, 'tlim': args.tlim, 'mlim': args.mlim, 'flim': args.flim }
    init_isohash(api, args.context, rlimits, args.query, args.hashes, target_db=args.target_db)


def cli_init_cnf2kis(api: GBD, args):
    from gbd_init.cnf_transformers import init_transform_cnf_to_kis
    rlimits = { 'jobs': args.jobs, 'tlim': args.tlim, 'mlim': args.mlim, 'flim': args.flim }
    init_transform_cnf_to_kis(api, args.context, rlimits, args.query, args.hashes, target_db=args.target_db)

def cli_init_sani(api: GBD, args):
    from gbd_init.cnf_transformers import init_sani
    rlimits = { 'jobs': args.jobs, 'tlim': args.tlim, 'mlim': args.mlim, 'flim': args.flim }
    init_sani(api, args.context, rlimits, args.query, args.hashes, target_db=args.target_db)


def cli_create(api: GBD, args):
    api.create_feature(args.name, args.unique, args.target_db)

def cli_delete(api: GBD, args):
    if args.hashes and len(args.hashes) > 0 or args.values and len(args.values):
        if args.force or util.confirm("Delete attributes of given hashes and/or values from '{}'?".format(args.name)):
            api.reset_values(args.name, args.values, args.hashes)
    elif args.force or util.confirm("Delete feature '{}' and all associated attributes?".format(args.name)):
        api.delete_feature(args.name)

def cli_rename(api: GBD, args):
    api.rename_feature(args.old_name, args.new_name)


def cli_get(api: GBD, args):
    df = api.query(args.query, args.hashes, args.resolve, args.collapse, args.group_by, args.join_type, args.subselect)
    for index, row in df.iterrows():
        print(" ".join([ item or "[None]" for item in row.to_list() ]))

def cli_set(api: GBD, args):
    hashes = api.query(args.query, args.hashes)['hash'].tolist()
    api.set_values(args.assign[0], args.assign[1], hashes)


def cli_info(api: GBD, args):
    if args.name is None:
        for dbname in api.get_databases():
            if len(api.get_features(dbname)):
                print("\nDatabase: {}".format(api.get_database_path(dbname)))
                feat = api.get_features(dbname)
                print("Features: " + " ".join(feat))
    else:
        info = api.get_feature_info(args.name)
        for key in info:
            print("{}: {}".format(key, info[key]))


### Define Command-Line Interface and Map Sub-Commands to Methods
def main():
    parser = get_gbd_argparser()

    subparsers = parser.add_subparsers(help='Available Commands:', required=True, dest='gbd command')

    # INITIALIZATION 
    parser_init = subparsers.add_parser('init', help='Initialize Database')
    add_resource_limits_arguments(parser_init)
    parser_init.add_argument('--target_db', help='Target database (default: first in list)', default=None)
    parser_init.add_argument('-c', '--context', default='cnf', choices=contexts.contexts(), help='Select context (affects selection of hash selection and initializers)')

    parser_init_subparsers = parser_init.add_subparsers(help='Select Initialization Procedure:', required=True, dest='init what?')

    # init local paths:
    parser_init_local = parser_init_subparsers.add_parser('local', help='Initialize Local Hash/Path Entries')
    parser_init_local.add_argument('path', type=directory_type, help="Path to benchmarks")
    parser_init_local.set_defaults(func=cli_init_local)
    # init base features:
    parser_init_base_features = parser_init_subparsers.add_parser('base_features', help='Initialize Base Features')
    add_query_and_hashes_arguments(parser_init_base_features)
    parser_init_base_features.set_defaults(func=cli_init_base_features)
    # init gate features:
    parser_init_gate_features = parser_init_subparsers.add_parser('gate_features', help='Initialize Gate Features')
    add_query_and_hashes_arguments(parser_init_gate_features)
    parser_init_gate_features.set_defaults(func=cli_init_gate_features)
    # generate kis instances from cnf instances:
    parser_init_cnf2kis = parser_init_subparsers.add_parser('cnf2kis', help='Generate KIS Instances from CNF Instances')
    add_query_and_hashes_arguments(parser_init_cnf2kis)
    parser_init_cnf2kis.set_defaults(func=cli_init_cnf2kis)
    # init iso-hash:
    parser_init_iso = parser_init_subparsers.add_parser('isohash', help='Initialize Isomorphic Hash (MD5 of sorted degree sequence)')
    add_query_and_hashes_arguments(parser_init_iso)
    parser_init_iso.set_defaults(func=cli_init_iso)
    # init sanitized:
    parser_init_sani = parser_init_subparsers.add_parser('sanitize', help='Initialize sanitized benchmarks')
    add_query_and_hashes_arguments(parser_init_sani)
    parser_init_sani.set_defaults(func=cli_init_sani)

    # GBD HASH
    parser_hash = subparsers.add_parser('hash', help='Print hash for a single file')
    parser_hash.add_argument('path', type=file_type, help="Path to one benchmark")
    parser_hash.set_defaults(func=cli_hash)

    # GBD GET $QUERY
    parser_get = subparsers.add_parser('get', help='Get data by query (or hash-list via stdin)')
    add_query_and_hashes_arguments(parser_get)
    parser_get.add_argument('-r', '--resolve', help='List of features to resolve against', nargs='+')
    parser_get.add_argument('-c', '--collapse', default='group_concat', 
                            choices=['group_concat', 'min', 'max', 'avg', 'count', 'sum', 'none'], 
                            help='Treatment of multiple values per hash (or grouping value resp.)')
    parser_get.add_argument('-g', '--group_by', default='hash', help='Group by specified attribute value')
    parser_get.add_argument('--subselect', help='Move where to subselect', action='store_true')
    parser_get.add_argument('--join-type', help='Join Type: treatment of missing values in queries', choices=['INNER', 'OUTER', 'LEFT'], default="LEFT")
    parser_get.set_defaults(func=cli_get)

    # GBD SET
    parser_set = subparsers.add_parser('set', help='Set specified attribute-value for query result')
    parser_set.add_argument('assign', type=key_value_type, help='key=value')
    add_query_and_hashes_arguments(parser_set)
    parser_set.set_defaults(func=cli_set)

    # CREATE/DELETE/MODIFY FEATURES
    parser_create = subparsers.add_parser('create', help='Create a new feature')
    parser_create.add_argument('name', type=column_type, help='Name of feature')
    parser_create.add_argument('-u', '--unique', help='Unique constraint: specify default-value of feature')
    parser_create.add_argument('--target_db', help='Target database (default: first in list)', default=None)
    parser_create.set_defaults(func=cli_create)

    parser_delete = subparsers.add_parser('delete', help='Delete all values assiociated with given hashes (via argument or stdin) or remove feature if no hashes are given')
    parser_delete.add_argument('--hashes', help='Hashes for which to delete values', nargs='*', default=[])
    parser_delete.add_argument('--values', help='Values to delete', nargs='*', default=[])
    parser_delete.add_argument('name', type=column_type, help='Name of feature')
    parser_delete.add_argument('-f', '--force', action='store_true', help='Do not ask for confirmation')
    parser_delete.set_defaults(func=cli_delete)

    parser_rename = subparsers.add_parser('rename', help='Rename feature')
    parser_rename.add_argument('old_name', type=column_type, help='Old name of feature')
    parser_rename.add_argument('new_name', type=column_type, help='New name of feature')
    parser_rename.set_defaults(func=cli_rename)

    # GET META INFO
    parser_info = subparsers.add_parser('info', help='Print info about available features')
    parser_info.add_argument('name', type=column_type, help='Print info about specified feature', nargs='?')
    parser_info.set_defaults(func=cli_info)

    # PARSE ARGUMENTS
    args = parser.parse_args()
    try:
        if hasattr(args, 'hashes') and not sys.stdin.isatty():
            if not args.hashes or len(args.hashes) == 0:
                args.hashes = util.read_hashes()  # read hashes from stdin
        if hasattr(args, 'target_db') and args.target_db is None:
            args.target_db = schema.Schema.dbname_from_path(args.db.split(os.pathsep)[0])
        with GBD(args.db.split(os.pathsep), args.verbose) as api:
            args.func(api, args)
    except ModuleNotFoundError as e:
        util.eprint("Module '{}' not found. Please install it.".format(e.name))
        if e.name == 'gbdc':
            util.eprint("You can install 'gbdc' from source: https://github.com/sat-clique/cnftools")
        sys.exit(1)
    except ParserException as e:
        util.eprint("Failed to parse query: " + args.query)
        if args.verbose:
            util.eprint(traceback.format_exc())
        sys.exit(1)
    except Exception as e:
        util.eprint("{}: {}".format(type(e), str(e)))
        if args.verbose:
            util.eprint(traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    main()
