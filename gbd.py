#!/usr/bin/python3
# -*- coding: utf-8 -*-

# GBD Benchmark Database (GBD)
# Copyright (C) 2021 Markus Iser, Karlsruhe Institute of Technology (KIT)
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

import os
import sys
import traceback

from gbd.api import GBD, GBDException
from gbd import contexts, util
from gbd.util_argparse import *
from gbd.init.cnf_hash import gbd_hash


### Command-Line Interface Entry Points
def cli_hash(api: GBD, args):
    print(gbd_hash.gbd_hash(args.path))


def cli_init_local(api: GBD, args):
    from gbd.init.cnf_hash import init_local
    init_local(api, args.path)

def cli_init_base_features(api: GBD, args):
    from gbd.init.cnf_base_features import init_base_features
    init_base_features(api, args.query, args.hashes)

def cli_init_gate_features(api: GBD, args):
    from gbd.init.cnf_gate_features import init_gate_features
    init_gate_features(api, args.query, args.hashes)

def cli_init_cnf2kis(api: GBD, args):
    from gbd.init.cnf_to_kis import init_transform_cnf_to_kis
    init_transform_cnf_to_kis(api, args.query, args.hashes, args.max_edges, args.max_nodes)

def cli_init_iso(api: GBD, args):
    from gbd.init.cnf_hash import init_iso_hash
    init_iso_hash(api, args.query, args.hashes)

def cli_init_sani(api: GBD, args):
    from gbd.init.cnf_sanitize import init_sani
    init_sani(api, args.query, args.hashes)


def cli_create(api: GBD, args):
    api.create_feature(args.name, args.unique)

def cli_delete(api: GBD, args):
    if args.hashes and len(args.hashes) > 0 or args.values and len(args.values):
        if args.force or util.confirm("Delete attributes of given hashes and/or values from '{}'?".format(args.name)):
            api.remove_attributes(args.name, args.values, args.hashes)
    elif args.force or util.confirm("Delete feature '{}' and all associated attributes?".format(args.name)):
        api.delete_feature(args.name)

def cli_rename(api: GBD, args):
    api.rename_feature(args.old_name, args.new_name)


def cli_get(api: GBD, args):
    df = api.query(args.query, args.hashes, args.resolve, args.collapse, args.group_by, args.subselect)
    for index, row in df.iterrows():
        print(" ".join([ item or "[None]" for item in row.to_list() ]))

def cli_set(api: GBD, args):
    api.set_attribute(args.assign[0], args.assign[1], None, args.hashes)


def cli_info(api: GBD, args):
    if args.name is None:
        for db_str in api.get_databases():
            if len(api.get_features(dbname=db_str)):
                print("\nDatabase: {}".format(api.get_database_path(db_str)))
                feat = api.get_material_features(dbname=db_str)
                if len(feat):
                    print("Features: " + " ".join(feat))
                feat = api.get_virtual_features(dbname=db_str)
                if len(feat):
                    print("Virtuals: " + " ".join(feat))
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
    parser_init_cnf2kis.add_argument('-e', '--max_edges', default=0, type=int, help='Maximum Number of Edges (0 = unlimited)')
    parser_init_cnf2kis.add_argument('-n', '--max_nodes', default=0, type=int, help='Maximum Number of Nodes (0 = unlimited)')
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
    parser_hash.add_argument('-c', '--context', default='cnf', choices=contexts.contexts(), 
                            help='Select context (affects hashes and features)')
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
        with GBD(args.db.split(os.pathsep), args.context, int(args.jobs), args.tlim, args.mlim, args.flim, args.join_type, args.verbose) as api:
            args.func(api, args)
    except ModuleNotFoundError as e:
        util.eprint("Module '{}' not found. Please install it.".format(e.name))
        if e.name == 'gdbc':
            util.eprint("You can install 'gdbc' from source: https://github.com/sat-clique/cnftools")
        sys.exit(1)
    except Exception as e:
        util.eprint("{}: {}".format(type(e), str(e)))
        if args.verbose:
            util.eprint(traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    main()
