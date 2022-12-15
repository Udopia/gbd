#!/usr/bin/python3
# GBD Benchmark Database (GBD)
# Copyright (C) 2021 Jakob Bach and Markus Iser, Karlsruhe Institute of Technology (KIT)
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

from gbd_core.api import GBD
from gbd_core import util
from gbd_core.util_argparse import *

from itertools import combinations
from operator import itemgetter
import traceback
import sys

import mip
import pandas as pd


def cli_eval_combinations_ilp(api: GBD, args):
    df = api.query(args.query, [], args.runtimes)
    df = df[args.runtimes][df.applymap(util.is_number)].applymap(float)
    df.fillna(2*args.tlim, inplace=True)
    model = mip.Model()
    instance_solver_vars = [[model.add_var(f'x_{i}_{j}', var_type=mip.BINARY)
                            for j in range(df.shape[1])] for i in range(df.shape[0])]
    solver_vars = [model.add_var(f's_{j}', var_type=mip.BINARY)for j in range(df.shape[1])]
    for var_list in instance_solver_vars:  # per-instance constraints
        model.add_constr(mip.xsum(var_list) == 1)
    for j in range(df.shape[1]):  # per-solver-constraints
        model.add_constr(mip.xsum(instance_solver_vars[i][j] for i in range(df.shape[0])) <= df.shape[0] * solver_vars[j])  # "Implies" in Z3
    model.add_constr(mip.xsum(solver_vars) <= args.size)
    model.objective = mip.minimize(mip.xsum(instance_solver_vars[i][j] * int(df.iloc[i, j])
                                            for i in range(df.shape[0]) for j in range(df.shape[1])))
    print("SBS Score: {}".format(df.sum().min() / len(df.index)))
    model.verbose = int(api.verbose)
    model.threads = api.jobs
    model.optimize()
    print("VBS Score (k={}): {}".format(args.size, model.objective_value / len(df.index)))
    for index, item in enumerate([var.x for var in solver_vars]):
        if item > 0:
            print(args.runtimes[index])

def cli_eval_combinations_greedy(api: GBD, args):
    df = api.query(args.query, [], args.runtimes)
    df[args.runtimes] = df[args.runtimes][df.applymap(util.is_number)].applymap(float)
    df.fillna(2*args.tlim, inplace=True)
    runtimes = ["dummy"] + args.runtimes
    for comb in combinations(range(1, len(runtimes)), args.size):
        comb_par2 = sum([min(itemgetter(*comb)(row)) for _, row in df.iterrows()]) / len(df.index)
        print(str(itemgetter(*comb)(runtimes)) + ": " + str(comb_par2))


### Define Command-Line Interface and Map Sub-Commands to Methods
def main():
    parser = get_gbd_argparser()

    subparsers = parser.add_subparsers(help='Available Commands:', required=True, dest='gbd command')

    parser_ilp = subparsers.add_parser('ilp', help='ILP Solution')
    add_query_and_hashes_arguments(parser_ilp)
    parser_ilp.add_argument('-r', '--runtimes', help='List of runtime features', nargs='+')
    parser_ilp.add_argument('-k', '--size', help='Portfolio Size', type=int, default=2)
    parser_ilp.set_defaults(func=cli_eval_combinations_ilp)

    parser_greedy = subparsers.add_parser('greedy', help='Greedy Solution')
    add_query_and_hashes_arguments(parser_greedy)
    parser_greedy.add_argument('-r', '--runtimes', help='List of runtime features', nargs='+')
    parser_greedy.add_argument('-k', '--size', help='Portfolio Size', type=int, default=2)
    parser_greedy.set_defaults(func=cli_eval_combinations_greedy)

    # PARSE ARGUMENTS
    args = parser.parse_args()
    try:
        if hasattr(args, 'hashes') and not sys.stdin.isatty():
            if not args.hashes or len(args.hashes) == 0:
                args.hashes = util.read_hashes()  # read hashes from stdin
        with GBD(args.db.split(os.pathsep), args.context, int(args.jobs), args.tlim, args.mlim, args.flim, args.join_type, args.verbose) as api:
            args.func(api, args)
    except Exception as e:
        util.eprint("{}: {}".format(type(e), str(e)))
        if args.verbose:
            util.eprint(traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    main()