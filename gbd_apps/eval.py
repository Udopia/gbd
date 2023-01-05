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
import gbd_core.util as util
from gbd_core.util_argparse import *

import traceback
import sys

from functools import reduce

def par2(series, timeout, divisor):
    return reduce(lambda x, y: x + (y if y < timeout else 2*timeout) / divisor, series, 0)

def cli_eval_par2(api: GBD, args):
    df = api.query(args.query, [], args.runtimes)
    df[args.runtimes] = df[args.runtimes][df.applymap(util.is_number)].applymap(float)
    df['vbs'] = df[args.runtimes].min(axis=1)
    div = len(df.index) if args.divisor is None else args.divisor
    timeout = args.tlim
    scores = df[args.runtimes].agg(par2, axis=0, timeout=timeout, divisor=div)
    print(scores)

def cli_eval_vbs(api: GBD, args):
    df = api.query(args.query, [], args.runtimes)
    df['vbs'] = df[args.runtimes].min(axis=1)
    for index, row in df.iterrows():
        print(row['hash'], row['vbs'])


### Define Command-Line Interface and Map Sub-Commands to Methods
def main():
    parser = get_gbd_argparser()

    subparsers = parser.add_subparsers(help='Available Commands:', required=True, dest='gbd command')

    parser_par2 = subparsers.add_parser('par2', help='Calculate Penalized Average Runtimes')
    add_query_and_hashes_arguments(parser_par2)
    parser_par2.add_argument('-r', '--runtimes', help='List of runtime features', nargs='+')
    parser_par2.add_argument('-d', '--divisor', help='Divisor for PAR2', type=int, default=None)
    parser_par2.set_defaults(func=cli_eval_par2)

    parser_vbs = subparsers.add_parser('vbs', help='Calculate Virtual Best Solver')
    add_query_and_hashes_arguments(parser_vbs)
    parser_vbs.add_argument('-r', '--runtimes', help='List of runtime features', nargs='+')
    parser_vbs.set_defaults(func=cli_eval_vbs)

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