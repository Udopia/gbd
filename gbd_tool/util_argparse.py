# GBD Benchmark Database (GBD)
# Copyright (C) 2022 Markus Iser, Karlsruhe Institute of Technology (KIT)
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
import multiprocessing

from gbd_tool import contexts

def get_gbd_argparser():
    parser = argparse.ArgumentParser(description='GBD Benchmark Database')

    parser.add_argument('-d', "--db", help='Specify database to work with', type=gbd_db_type, nargs='?', default=os.environ.get('GBD_DB'))
    parser.add_argument('-j', "--jobs", help='Specify number of parallel jobs', default=1, type=jobs_type, nargs='?')
    parser.add_argument('-v', '--verbose', help='Print additional (or diagnostic) information to stderr', action='store_true')
    parser.add_argument('-w', '--subselect', help='Move where to subselect', action='store_true')

    parser.add_argument('-t', '--tlim', help="Time limit (sec) per instance for 'init' sub-commands (also used for score calculation in 'eval' and 'plot')", default=5000, type=int)
    parser.add_argument('-m', '--mlim', help="Memory limit (MB) per instance for 'init' sub-commands", default=2000, type=int)
    parser.add_argument('-f', '--flim', help="File size limit (MB) per instance for 'init' sub-commands which create files", default=1000, type=int)

    parser.add_argument('-s', "--separator", help="Feature separator (delimiter used in import and output", choices=[" ", ",", ";"], default=" ")
    parser.add_argument("--join-type", help="Join Type: treatment of missing values in queries", choices=["INNER", "OUTER", "LEFT"], default="LEFT")
    parser.add_argument('-c', '--context', default='cnf', choices=contexts.contexts(), 
                            help='Select context (affects selection of hash/identifier and available feature-extractors in init)')

    return parser

### Argument Types for Input Sanitation in ArgParse Library
def directory_type(path):
    if not os.path.isdir(path):
        raise argparse.ArgumentTypeError('{0} is not a directory'.format(path))
    if os.access(path, os.R_OK):
        return os.path.abspath(path)
    else:
        raise argparse.ArgumentTypeError('{0} is not readable'.format(path))

def file_type(path):
    if not os.path.isfile(path):
        raise argparse.ArgumentTypeError('{0} is not a regular file'.format(path))
    if os.access(path, os.R_OK):
        return os.path.abspath(path)
    else:
        raise argparse.ArgumentTypeError('{0} is not readable'.format(path))

def column_type(s):
    pat = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]*$")
    if not pat.match(s):
        raise argparse.ArgumentTypeError('Column "{0}" does not match regular expression {1}'.format(s, pat.pattern))
    return s

def key_value_type(s):
    tup = s.split('=', 1)
    if len(tup) != 2:
        raise argparse.ArgumentTypeError('key-value type: {0} must be separated by exactly one = '.format(s))
    return (column_type(tup[0]), tup[1])

def gbd_db_type(dbstr):
    if not dbstr:
        default=os.environ.get('GBD_DB')
        if not default:
            raise argparse.ArgumentTypeError("Datasources Missing: Set GBD_DB environment variable (Get databases: http://gbd.iti.kit.edu/)")
        return default
    return dbstr

def jobs_type(jobs):
    val = int(jobs)
    if val >= 1 and val <= multiprocessing.cpu_count():
        return val
    else:
        raise argparse.ArgumentTypeError('number of jobs not accepted')

def add_query_and_hashes_arguments(parser: argparse.ArgumentParser):
    parser.add_argument('query', help='GBD Query', nargs='?')
    parser.add_argument('--hashes', help='Give Hashes as ARGS or via STDIN', nargs='*', default=[])


