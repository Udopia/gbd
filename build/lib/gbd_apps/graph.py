#!/usr/bin/python3

from gbd_core.api import GBD
from gbd_core import util
from gbd_core.util_argparse import *

from graph_tool.all import *
from pysat.formula import CNF

import itertools
import traceback
import os
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



### Define Command-Line Interface and Map Sub-Commands to Methods
def main():
    parser = get_gbd_argparser()

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
        with GBD(args.db.split(os.pathsep), args.context, int(args.jobs), args.tlim, args.mlim, args.flim, args.join_type, args.verbose) as api:
            args.func(api, args)
    except Exception as e:
        util.eprint("{}: {}".format(type(e), str(e)))
        if args.verbose:
            util.eprint(traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    main()
