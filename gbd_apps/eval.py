#!/usr/bin/python3
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

from gbd.api import GBD
import gbd.util as util
from gbd.util_argparse import *

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
        with GBD(args.db.split(os.pathsep), args.context, int(args.jobs), args.tlim, args.mlim, args.flim, args.join_type, args.verbose) as api:
            args.func(api, args)
    except Exception as e:
        util.eprint("{}: {}".format(type(e), str(e)))
        if args.verbose:
            util.eprint(traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    main()