from gbd_tool.gbd_api import GBD
from gbd_tool import contexts, util

from graph_tool.all import *
from pysat.formula import CNF

import argparse
import itertools
import traceback
import os
import re
import sys


def gen_vig(path):
    g = Graph(directed=False)
    cnf = CNF()
    cnf.from_file(path, compressed_with='lzma')
    print("NVars: {}".format(cnf.nv))
    g.add_vertex(cnf.nv + 1)
    w = g.new_edge_property("float")
    for clause in cnf:
        inc = 1.0 / pow(2, len(clause))
        for tup in itertools.combinations(clause, 2):
            e = g.edge(g.vertex(abs(tup[0])), g.vertex(abs(tup[1])), add_missing=True)
            w[e] = w[e] + inc
    return (g, w)


def vig(api: GBD, path, proof):
    (g, w) = gen_vig(path)
    pos = sfdp_layout(g, eweight=w)
    graph_draw(g, pos, output_size=(1000, 1000), vertex_color=[1,1,1,0],
           vertex_size=1, edge_pen_width=1.2, output="graph.pdf")

def read_by_tokens(fileobj):
    for line in fileobj:
        for token in line.split():
            yield token

from gi.repository import Gtk, Gdk, GdkPixbuf, GObject, GLib

def animate_proof(api: GBD, path, proof):
    (g, w) = gen_vig(path)
    pos = sfdp_layout(g, eweight=w)
    win = GraphWindow(g, pos, geometry=(800, 600))

    handle = open(proof)        
    def update(handle):
        delete = False
        clause = []
        sfdp_layout(g, pos=pos, K=0.5 , init_step=0.005, max_iter=1)
        for token in read_by_tokens(handle):
            if token == "d":
                delete = True
            elif token == "0":
                inc = 1.0 / pow(2, len(clause))
                if delete:
                    for tup in itertools.combinations(clause, 2):
                        e = g.edge(g.vertex(abs(tup[0])), g.vertex(abs(tup[1])))
                        w[e] = w[e] - inc
                else:
                    for tup in itertools.combinations(clause, 2):
                        e = g.edge(g.vertex(abs(tup[0])), g.vertex(abs(tup[1])))
                        w[e] = w[e] + inc
                print(clause)
                win.graph.regenerate_surface()
                win.graph.queue_draw()
                return True
            else:
                clause = clause + [int(token)]
        return False
        
    cid = GLib.idle_add(update, handle)
   
    # We will give the user the ability to stop the program by closing the window.

    win.connect("delete_event", Gtk.main_quit)


    # Actually show the window, and start the main loop.

    win.show_all()

    Gtk.main()


def cli_graph(api: GBD, args):
    from gbd_apps import graph
    graph.animate_proof(api, args.path, args.proof)

def cli_merge_contexts(api: GBD, args):
    from gbd_tool import contexts
    contexts.merge(api, args.source, args.target)

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

def add_query_and_hashes_arguments(parser: argparse.ArgumentParser):
    parser.add_argument('query', help='GBD Query', nargs='?')
    parser.add_argument('--hashes', help='Give Hashes as ARGS or via STDIN', nargs='*', default=[])



### Define Command-Line Interface and Map Sub-Commands to Methods
def main():
    parser = argparse.ArgumentParser(description='GBD Benchmark Database')

    parser.add_argument('-d', "--db", help='Specify database to work with', type=gbd_db_type, nargs='?', default=os.environ.get('GBD_DB'))
    parser.add_argument('-v', '--verbose', help='Print additional (or diagnostic) information to stderr', action='store_true')
    parser.add_argument('-w', '--subselect', help='Move where to subselect', action='store_true')

    parser.add_argument('-t', '--tlim', help="Time limit (sec) per instance for 'init' sub-commands", default=5000, type=int)
    parser.add_argument('-m', '--mlim', help="Memory limit (MB) per instance for 'init' sub-commands", default=2000, type=int)
    parser.add_argument('-f', '--flim', help="File size limit (MB) per instance for 'init' sub-commands which create files", default=1000, type=int)

    parser.add_argument('-s', "--separator", help="Feature separator (delimiter used in import and output", choices=[" ", ",", ";"], default=" ")
    parser.add_argument("--join-type", help="Join Type: treatment of missing values in queries", choices=["INNER", "OUTER", "LEFT"], default="LEFT")
    parser.add_argument('-c', '--context', default='cnf', choices=contexts.contexts(), 
                            help='Select context (affects selection of hash/identifier and available feature-extractors in init)')

    subparsers = parser.add_subparsers(help='Available Commands:', required=True, dest='gbd command')

    # GRAPHS
    parser_graph = subparsers.add_parser('graph', help='Visualize Formula')
    parser_graph.add_argument('path', type=file_type, help='CNF File')
    parser_graph.add_argument('proof', type=file_type, help='Proof File')
    parser_graph.set_defaults(func=cli_graph)

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
