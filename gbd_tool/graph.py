from gbd_tool.gbd_api import GBD

from graph_tool.all import *
from pysat.formula import CNF

import itertools


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

