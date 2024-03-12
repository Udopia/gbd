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
from gbd_init.feature_extractors import generic_extractors


### Command-Line Interface Entry Points
def cli_hash(api: GBD, args):
    from gbd_core.contexts import identify
    print(identify(args.path))


def cli_init_local(api: GBD, args):
    from gbd_init.feature_extractors import init_local
    rlimits = { 'jobs': args.jobs, 'tlim': args.tlim, 'mlim': args.mlim, 'flim': args.flim }
    init_local(api, rlimits, args.path, args.target)


def cli_init_generic(api: GBD, args):
    from gbd_init.feature_extractors import init_features_generic
    rlimits = { 'jobs': args.jobs, 'tlim': args.tlim, 'mlim': args.mlim, 'flim': args.flim }
    context = api.database.dcontext(args.target)
    df = api.query(args.query, args.hashes, [ context + ":local" ], collapse="MIN", group_by=context + ":hash")
    init_features_generic(args.initfuncname, api, rlimits, df, args.target)


def cli_trans_cnf2kis(api: GBD, args):
    from gbd_init.benchmark_transformers import init_transform_cnf_to_kis
    rlimits = { 'jobs': args.jobs, 'tlim': args.tlim, 'mlim': args.mlim, 'flim': args.flim }
    init_transform_cnf_to_kis(api, rlimits, args.query, args.hashes, args.target, args.source)

def cli_trans_sani(api: GBD, args):
    from gbd_init.benchmark_transformers import init_sani
    rlimits = { 'jobs': args.jobs, 'tlim': args.tlim, 'mlim': args.mlim, 'flim': args.flim }
    init_sani(api, rlimits, args.query, args.hashes, args.target, args.source)


def cli_create(api: GBD, args):
    api.create_feature(args.name, args.unique, args.target)

def cli_delete(api: GBD, args):
    if args.hashes and len(args.hashes) or args.values and len(args.values):
        if args.force or util.confirm("Delete attributes of given hashes and/or values from '{}'?".format(args.name)):
            api.reset_values(args.name, args.values, args.hashes)
    elif args.force or util.confirm("Delete feature '{}' and all associated attributes?".format(args.name)):
        api.delete_feature(args.name)

def cli_cleanup(api: GBD, args):
    if args.hashes and len(args.hashes):
        if args.force or util.confirm("Delete attributes of given hashes from all features?"):
            api.delete_hashes(args.hashes, args.target)

def cli_rename(api: GBD, args):
    api.rename_feature(args.old_name, args.new_name)

def cli_copy(api: GBD, args):
    api.copy_feature(args.old_name, args.new_name, args.target, args.query, args.hashes)


def cli_get(api: GBD, args):
    df = api.query(args.query, args.hashes, args.resolve, args.collapse, args.group_by, args.join_type)
    if args.header:
        print(args.delimiter.join(df.columns))
    for index, row in df.iterrows():
        print(args.delimiter.join([ item or "[None]" for item in row.to_list() ]))

def cli_set(api: GBD, args):
    hashes = api.query(args.query, args.hashes)['hash'].tolist()
    if args.create:
        hashes = list(set(hashes + args.hashes))
    if len(hashes) > 0:
        api.set_values(args.assign[0], args.assign[1], hashes)


def cli_info(api: GBD, args):
    if args.contexts:
        print("# Available Contexts: " + ", ".join(contexts.contexts()))
        for context in contexts.contexts():
            print()
            print("## " + contexts.context_data[context]['description'])
            print(" - Context Prefix: " + context)
            print(" - File Extensions: " + ",".join(contexts.suffix_list(context)))
    elif args.name is None:
        print("# Available Data Sources: " + ", ".join(api.get_databases()))
        for dbname in api.get_databases():
            if len(api.get_features(dbname)):
                print()
                print("## " + api.get_database_path(dbname))
                print(" - Name: " + dbname)
                feat = api.get_features(dbname)
                print(" - Features: " + " ".join(feat))
                if args.verbose:
                    for f in feat:
                        info = api.database.find(":".join([ dbname, f ]))
                        print(info)
    else:
        info = api.get_feature_info(args.name)
        for key in info:
            print("{}: {}".format(key, info[key]))


def cli_server(api: GBD, args):
    from gbd_server import server
    util.eprint("Starting GBD Server on port {}...".format(args.port))
    util.eprint(r'''
Warning: All files referenced in the configured databases are now accessible on the specified port.
If you do not trust the source of the databases, do not run the server.
''')
    server.serve(api, args.port, args.logdir)


### Define Command-Line Interface and Map Sub-Commands to Methods
def main():
    parser = get_gbd_argparser()

    subparsers = parser.add_subparsers(help='Available Commands:', required=True, dest='gbd command')

    # INITIALIZATION 
    parser_init = subparsers.add_parser('init', help='Initialize Database')
    add_resource_limits_arguments(parser_init)
    parser_init.add_argument('--target', help='Target database for new features (default: first db in list); also determines target context', default=None)

    parser_init_subparsers = parser_init.add_subparsers(help='Select Initialization Procedure:', required=True, dest='init what?')

    # init local paths:
    parser_init_local = parser_init_subparsers.add_parser('local', help='Initialize Local Hash/Path Entries')
    parser_init_local.add_argument('path', type=directory_type, help="Path to benchmarks")
    parser_init_local.set_defaults(func=cli_init_local)

    # hooks for generic feature extractors:
    for key in generic_extractors.keys():
        parser_init_generic = parser_init_subparsers.add_parser(key, help='Initialize Featureset {}, valid contexts are: {}'.format(key, ", ".join(generic_extractors[key]["contexts"])))
        add_query_and_hashes_arguments(parser_init_generic)
        parser_init_generic.set_defaults(func=cli_init_generic, initfuncname=key)

    # TRANSFORMATION
    parser_trans = subparsers.add_parser('transform', help='Transform Benchmarks')
    add_resource_limits_arguments(parser_trans)
    parser_trans.add_argument('--source', help='Source context', default='cnf')
    parser_trans.add_argument('--target', help='Target database; determines target context (default: first db in list)', default=None)

    parser_trans_subparsers = parser_trans.add_subparsers(help='Select Transformation Procedure:', required=True, dest='transform how?')
    
    # generate kis instances from cnf instances:
    parser_trans_cnf2kis = parser_trans_subparsers.add_parser('cnf2kis', help='Generate KIS Instances from CNF Instances')
    add_query_and_hashes_arguments(parser_trans_cnf2kis)
    parser_trans_cnf2kis.set_defaults(func=cli_trans_cnf2kis)
    # init sanitized:
    parser_trans_sani = parser_trans_subparsers.add_parser('sanitize', help='Initialize sanitized benchmarks')
    add_query_and_hashes_arguments(parser_trans_sani)
    parser_trans_sani.set_defaults(func=cli_trans_sani)

    # GBD HASH
    parser_hash = subparsers.add_parser('hash', help='Print hash for a single file')
    parser_hash.add_argument('path', type=file_type, help="Path to one benchmark")
    parser_hash.set_defaults(func=cli_hash)

    # GBD GET $QUERY
    parser_get = subparsers.add_parser('get', help='Get data by query (or hash-list via stdin)')
    add_query_and_hashes_arguments(parser_get)
    parser_get.add_argument('-r', '--resolve', help='List of features to resolve against', nargs='+', default=[])
    parser_get.add_argument('-c', '--collapse', default='group_concat', 
                            choices=['group_concat', 'min', 'max', 'avg', 'count', 'sum', 'none'], 
                            help='Treatment of multiple values per hash (or grouping value resp.)')
    parser_get.add_argument('-g', '--group_by', default=None, help='Group by specified attribute value')
    parser_get.add_argument('--join-type', help='Join Type: treatment of missing values in queries', choices=['INNER', 'OUTER', 'LEFT'], default="LEFT")
    parser_get.add_argument('-d', '--delimiter', default=' ', help='CSV delimiter to use in output')
    parser_get.add_argument('-H', '--header', action='store_true', help='Include header information in output')
    parser_get.set_defaults(func=cli_get)

    # GBD SET
    parser_set = subparsers.add_parser('set', help='Set specified attribute-value for query result')
    parser_set.add_argument('assign', type=key_value_type, help='key=value')
    parser_set.add_argument('-c', '--create', help='Create given hashes if they do not exist yet (otherwise intersect with existing hashes)', action='store_true')
    add_query_and_hashes_arguments(parser_set)
    parser_set.set_defaults(func=cli_set)

    # CREATE/DELETE/MODIFY FEATURES
    parser_create = subparsers.add_parser('create', help='Create a new feature')
    parser_create.add_argument('name', type=column_type, help='Name of feature')
    parser_create.add_argument('-u', '--unique', help='Unique constraint: specify default-value of feature')
    parser_create.add_argument('--target', help='Target database (default: first in list)', default=None)
    parser_create.set_defaults(func=cli_create)

    parser_delete = subparsers.add_parser('delete', help='Delete all values assiociated with given hashes (via argument or stdin) or remove feature if no hashes are given')
    parser_delete.add_argument('--hashes', help='Hashes for which to delete values', nargs='*', default=[])
    parser_delete.add_argument('--values', help='Values to delete', nargs='*', default=[])
    parser_delete.add_argument('name', type=column_type, help='Name of feature')
    parser_delete.add_argument('-f', '--force', action='store_true', help='Do not ask for confirmation')
    parser_delete.set_defaults(func=cli_delete)

    parser_cleanup = subparsers.add_parser('cleanup', help='Delete given hashes from all features')
    parser_cleanup.add_argument('--hashes', help='Hashes for which to delete values', nargs='*', default=[])
    parser_cleanup.add_argument('-f', '--force', action='store_true', help='Do not ask for confirmation')
    parser_cleanup.add_argument('--target', help='Target database (default: first in list)', default=None)
    parser_cleanup.set_defaults(func=cli_cleanup)

    parser_rename = subparsers.add_parser('rename', help='Rename feature')
    parser_rename.add_argument('old_name', type=column_type, help='Old name of feature')
    parser_rename.add_argument('new_name', type=column_type, help='New name of feature')
    parser_rename.set_defaults(func=cli_rename)

    parser_copy = subparsers.add_parser('copy', help='Copy feature')
    add_query_and_hashes_arguments(parser_copy)
    parser_copy.add_argument('--target', help='Target database (default: first in list)', default=None)
    parser_copy.add_argument('old_name', type=column_type, help='Old name of feature')
    parser_copy.add_argument('new_name', type=column_type, help='New name of feature')
    parser_copy.set_defaults(func=cli_copy)

    # GET META INFO
    parser_info = subparsers.add_parser('info', help='Print info about available features')
    parser_info.add_argument('-c', '--contexts', action='store_true', help='Print available contexts')
    parser_info.add_argument('name', type=column_type, help='Print info about specified feature', nargs='?')
    parser_info.set_defaults(func=cli_info)

    # RUN SERVER
    parser_server = subparsers.add_parser('serve', help='Run GBD Server')
    parser_server.add_argument('-p', "--port", help='Specify port on which to listen', default=os.environ.get('GBD_PORT') or 5000, type=int)
    parser_server.add_argument('-l', "--logdir", help='Specify directory for logfiles', default=os.environ.get('GBD_LOGS') or "./")
    parser_server.set_defaults(func=cli_server)

    # PARSE ARGUMENTS
    args = parser.parse_args()
    try:
        if hasattr(args, 'hashes') and not sys.stdin.isatty():
            if not args.hashes or len(args.hashes) == 0:
                args.hashes = util.read_hashes()  # read hashes from stdin
        if hasattr(args, 'target') and args.target is None:
            args.target = schema.Schema.dbname_from_path(args.db.split(os.pathsep)[0])
        with GBD(args.db.split(os.pathsep), args.verbose) as api:
            args.func(api, args)
    except ModuleNotFoundError as e:
        util.eprint("Module '{}' not found. Please install it.".format(e.name))
        if e.name == 'gbdc':
            util.eprint("You can install 'gbdc' from source: https://github.com/Udopia/gbdc")
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
