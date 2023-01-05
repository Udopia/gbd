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
        with GBD(args.db.split(os.pathsep), args.verbose) as api:
            args.func(api, args)
    except Exception as e:
        util.eprint("{}: {}".format(type(e), str(e)))
        if args.verbose:
            util.eprint(traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    main()