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

def par2(api: GBD, query, runtimes, timeout, divisor):
    for name in runtimes:
        times = api.query_search(query, [], [name])
        div = len(times) if divisor is None else divisor
        par2 = sum(float(time[1]) if util.is_number(time[1]) and float(time[1]) < timeout else 2*timeout for time in times) / div
        solved = sum(1 if util.is_number(time[1]) and float(time[1]) < timeout else 0 for time in times)
        print(str(round(par2, 2)) + " " + str(solved) + "/" + str(div) + " " + name)
    times = api.query_search(query, [], runtimes)
    div = len(times) if divisor is None else divisor
    vbs_par2 = sum([min(float(val) if util.is_number(val) and float(val) < timeout else 2*timeout for val in row[1:]) for row in times]) / div
    solved = sum(1 if t < timeout else 0 for t in [min(float(val) if util.is_number(val) else 2*timeout for val in row[1:]) for row in times])
    print(str(round(vbs_par2, 2)) + " " + str(solved) + "/" + str(div) + " VBS")

def vbs(api: GBD, query, runtimes, timeout, separator):
    result = api.query_search(query, [], runtimes)
    resultset = [(row[0], min(float(val) if util.is_number(val) else 2*timeout for val in row[1:])) for row in result]
    for result in resultset:
        print(separator.join([(str(item or '')) for item in result]))

def cli_eval_par2(api: GBD, args):
    par2(api, args.query, args.runtimes, args.tlim, args.divisor)

def cli_eval_vbs(api: GBD, args):
    vbs(api, args.query, args.runtimes, args.tlim, args.separator)


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
        with GBD(args.db.split(os.pathsep), args.context, int(args.jobs), args.tlim, args.mlim, args.flim, args.separator, args.join_type, args.verbose) as api:
            args.func(api, args)
    except Exception as e:
        util.eprint("{}: {}".format(type(e), str(e)))
        if args.verbose:
            util.eprint(traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    main()