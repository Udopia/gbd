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
import sys

from gbd_tool.gbd_api import GBD, GBDException
from gbd_tool.gbd_hash import gbd_hash
from gbd_tool.util import eprint, read_hashes, confirm

import gbd_tool.eval as eval
import gbd_tool.eval_comb_ilp as eci
import gbd_tool.plot as plot
import gbd_tool.graph as graph
import gbd_tool.init as init



### Command-Line Interface Entry Points
def cli_hash(api: GBD, args):
    path = os.path.abspath(args.path)
    print(gbd_hash(path))


def cli_import(api: GBD, args):
    path = os.path.abspath(args.path)
    init.import_csv(api, path, args.key, args.source, args.target)

def cli_init_local(api: GBD, args):
    init.init_local(api, os.path.abspath(args.path))

def cli_init_ct(api: GBD, args):
    init.init_clause_types(api, args.hashes)

def cli_init_dsh(api: GBD, args):
    init.init_degree_sequence_hash(api, args.hashes)


def cli_create(api: GBD, args):
    api.create_feature(args.name, args.unique)

def cli_delete(api: GBD, args):
    if args.optional_hashes and len(args.optional_hashes) > 0:
        if args.force or confirm("Delete attributes of given hashes from '{}'?".format(args.name)):
            api.remove_attributes(args.name, args.optional_hashes)
    elif args.force or confirm("Delete feature '{}' and all associated attributes?".format(args.name)):
        api.remove_feature(args.name)

def cli_rename(api: GBD, args):
    api.rename_feature(args.old_name, args.new_name)

def cli_get(api: GBD, args):
    resultset = api.query_search(args.query, args.optional_hashes, args.resolve, args.collapse, args.group_by)
    for result in resultset:
        print(args.separator.join([(str(item or '')) for item in result]))

def cli_set(api: GBD, args):
    api.set_attribute(args.assign[0], args.assign[1], None, args.hashes, args.force)

def cli_info_set(api: GBD, args):
    api.meta_set(args.feature, args.name, args.value)

def cli_info_clear(api: GBD, args):
    api.meta_clear(args.feature, args.name)

def cli_info(api: GBD, args):
    if args.name is None:
        for db_str in api.get_databases():
            print("Database: {}".format(db_str))
            print("Features: {}".format(" ".join(api.get_material_features(db_str))))
            print("Virtual: {}".format(" ".join(api.get_virtual_features(db_str))))
    else:
        info = api.get_feature_info(args.name)
        for key in info:
            print("{}: {}".format(key, info[key]))

def cli_eval_par2(api: GBD, args):
    eval.par2(api, args.query, args.runtimes, args.timeout, args.divisor)

def cli_eval_vbs(api: GBD, args):
    eval.vbs(api, args.query, args.runtimes, args.timeout, args.separator)

def cli_eval_combinations(api: GBD, args):
    #eval.greedy_comb(api, args.query, args.runtimes, args.timeout, args.size)
    eci.optimal_comb(api, args.query, args.runtimes, args.timeout, args.size)

def cli_graph(api: GBD, args):
    graph.animate_proof(api, args.path, args.proof)

def cli_plot_scatter(api: GBD, args):
    plot.scatter(api, args.query, args.runtimes, args.timeout, args.groups)

def cli_plot_cdf(api: GBD, args):
    plot.cdf(api, args.query, args.runtimes, args.timeout, args.title)

def cli_extract(api: GBD, args):
    api.extract_base_features(args.path)


### Argument Types for Input Sanitation in ArgParse Library
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
        raise argparse.ArgumentTypeError('Column "{0}" does not match regular expression {1}'.format(s, pat.pattern))
    return s

def key_value_type(s):
    tup = s.split('=', 2)
    if len(tup) != 2:
        raise argparse.ArgumentTypeError('key-value type: {0} must be separated by exactly one = '.format(s))
    return (column_type(tup[0]), tup[1])

def add_query_and_hashes_arguments(parser: argparse.ArgumentParser, hashes_are_optional = True):
    parser.add_argument('query', help='GBD Query', nargs='?')
    if hashes_are_optional:
        parser.add_argument('--hashes', dest='optional_hashes', help='Give Hashes as ARGS or via STDIN', nargs='*', default=[])
    else:
        parser.add_argument('--hashes', help='Give Hashes as ARGS or via STDIN', nargs='*', default=[])



### Define Command-Line Interface and Map Sub-Commands to Methods
def main():
    parser = argparse.ArgumentParser(description='GBD Benchmark Database')

    parser.add_argument('-d', "--db", help='Specify database to work with', default=os.environ.get('GBD_DB'), nargs='?')
    parser.add_argument('-j', "--jobs", help='Specify number of parallel jobs', default=1, nargs='?')
    parser.add_argument('-s', "--separator", choices=[" ", ",", ";"], default=" ", help="Feature separator (delimiter used in import and output)")
    parser.add_argument('-t', "--join-type", choices=["INNER", "OUTER", "LEFT"], default="LEFT", help="Join Type: treatment of missing values in queries")
    parser.add_argument('-v', '--verbose', action='store_true', help='Print additional (or diagnostic) information to stderr')

    subparsers = parser.add_subparsers(help='Available Commands:')

    # INITIALIZATION 
    parser_init = subparsers.add_parser('init', help='Initialize Database')
    parser_init_subparsers = parser_init.add_subparsers(help='Select Initialization Procedure:')
    # init local paths:
    parser_init_local = parser_init_subparsers.add_parser('local', help='Initialize Local Hash/Path Entries')
    parser_init_local.add_argument('path', type=directory_type, help="Path to benchmarks")
    parser_init_local.set_defaults(func=cli_init_local)
    # init clause types:
    parser_init_ct = parser_init_subparsers.add_parser('clause_types', help='Initialize Clause-Type Tables')
    add_query_and_hashes_arguments(parser_init_ct)
    parser_init_ct.set_defaults(func=cli_init_ct)
    # init degree_sequence_hash:
    parser_init_dsh = parser_init_subparsers.add_parser('degree_sequence_hash', help='Initialize Degree-Sequence Hash')
    add_query_and_hashes_arguments(parser_init_dsh)
    parser_init_dsh.set_defaults(func=cli_init_dsh)

    # GBD HASH
    parser_hash = subparsers.add_parser('hash', help='Print hash for a single file')
    parser_hash.add_argument('path', type=file_type, help="Path to one benchmark")
    parser_hash.set_defaults(func=cli_hash)

    # GBD GET $QUERY
    parser_get = subparsers.add_parser('get', help='Get data by query (or hash-list via stdin)')
    add_query_and_hashes_arguments(parser_get)
    parser_get.add_argument('-r', '--resolve', help='List of features to resolve against', nargs='+')
    parser_get.add_argument('-c', '--collapse', default='group_concat', 
                            choices=['group_concat', 'min', 'max', 'avg', 'count', 'sum'], 
                            help='Treatment of multiple values per hash (or grouping value resp.)')
    parser_get.add_argument('-g', '--group_by', default='hash', help='Group by specified attribute value')
    parser_get.set_defaults(func=cli_get)

    # GBD SET
    parser_set = subparsers.add_parser('set', help='Set specified attribute-value for query result')
    parser_set.add_argument('assign', type=key_value_type, help='key=value')
    add_query_and_hashes_arguments(parser_set)
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
    parser_delete.add_argument('--hashes', dest='optional_hashes', help='Hashes', nargs='*', default=[])
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
    add_query_and_hashes_arguments(parser_eval_par2)
    parser_eval_par2.add_argument('-r', '--runtimes', help='List of runtime features', nargs='+')
    parser_eval_par2.add_argument('-t', '--timeout', default=5000, type=int, help='Timeout')
    parser_eval_par2.add_argument('-d', '--divisor', type=int, help='Overwrite Divisor used for Averaging Scores', nargs='?')
    parser_eval_par2.set_defaults(func=cli_eval_par2)

    parser_eval_vbs = parser_eval_subparsers.add_parser('vbs', help='Calculate VBS')
    add_query_and_hashes_arguments(parser_eval_vbs)
    parser_eval_vbs.add_argument('-r', '--runtimes', help='List of runtime features', nargs='+')
    parser_eval_vbs.add_argument('-t', '--timeout', default=5000, type=int, help='Timeout')
    parser_eval_vbs.set_defaults(func=cli_eval_vbs)

    parser_eval_comb = parser_eval_subparsers.add_parser('comb', help='Calculate VBS of Solver Combinations')
    add_query_and_hashes_arguments(parser_eval_comb)
    parser_eval_comb.add_argument('-k', '--size', default=2, type=int, help='Number of Solvers per Combination')
    parser_eval_comb.add_argument('-r', '--runtimes', help='List of runtime features', nargs='+')
    parser_eval_comb.add_argument('-t', '--timeout', default=5000, type=int, help='Timeout')
    parser_eval_comb.set_defaults(func=cli_eval_combinations)

    # PLOTS
    parser_plot = subparsers.add_parser('plot', help='Plot Runtimes')
    parser_plot_subparsers = parser_plot.add_subparsers(help='Select Plot')

    parser_plot_scatter = parser_plot_subparsers.add_parser('scatter', help='Scatter Plot')
    add_query_and_hashes_arguments(parser_plot_scatter)
    parser_plot_scatter.add_argument('-r', '--runtimes', help='Two runtime features', nargs=2)
    parser_plot_scatter.add_argument('-g', '--groups', help='Highlight specific groups (e.g. family=cryptography)', nargs='+')
    parser_plot_scatter.add_argument('-t', '--timeout', default=5000, type=int, help='Timeout')
    parser_plot_scatter.set_defaults(func=cli_plot_scatter)

    parser_plot_cdf = parser_plot_subparsers.add_parser('cdf', help='CDF Plot')
    add_query_and_hashes_arguments(parser_plot_cdf)
    parser_plot_cdf.add_argument('-r', '--runtimes', help='List of runtime features', nargs='+')
    parser_plot_cdf.add_argument('-t', '--timeout', default=5000, type=int, help='Timeout')
    parser_plot_cdf.add_argument('--title', help='Plot Title')
    parser_plot_cdf.set_defaults(func=cli_plot_cdf)

    # GRAPHS
    parser_graph = subparsers.add_parser('graph', help='Visualize Formula')
    parser_graph.add_argument('path', type=file_type, help='CNF File')
    parser_graph.add_argument('proof', type=file_type, help='Proof File')
    parser_graph.set_defaults(func=cli_graph)

    # EXTRACT
    parser_extract = subparsers.add_parser('extract', help='Extract Features')
    parser_extract.add_argument('path', type=file_type, help='CNF File')
    parser_extract.set_defaults(func=cli_extract)

    # PARSE ARGUMENTS
    args = parser.parse_args()
    if not args.db:
            eprint("""Error: No database path is given. 
A database path can be given in two ways:
-- by setting the environment variable GBD_DB
-- by giving a path via --db=[path]
A database file containing some attributes of instances used in the SAT Competitions can be obtained at http://gbd.iti.kit.edu/getdatabase""")
    elif len(sys.argv) > 1:
        try:
            with GBD(args.db, int(args.jobs), args.separator, args.join_type, args.verbose) as api:
                if hasattr(args, 'hashes') and (not args.hashes or len(args.hashes) == 0):
                    if not sys.stdin.isatty():
                        args.hashes = read_hashes()  # read hashes from stdin
                    if not args.hashes or len(args.hashes) == 0:
                        raise GBDException("Error: No hashes given. Enter hashes via STDIN or ARGUMENT --hashes [hash1 [hash2 [...]]]")
                if hasattr(args, 'optional_hashes') and (not args.optional_hashes or len(args.optional_hashes) == 0) and not sys.stdin.isatty():
                    args.optional_hashes = read_hashes()  # read hashes from stdin
                args.func(api, args)
        except GBDException as err:
            eprint(err)
            sys.exit(1)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
