from gbd_tool.gbd_api import GbdApi

from graph_tool.all import *
from pysat.formula import CNF

import itertools

def cli_graph(api: GbdApi, args):
    g = Graph(directed=False)
    cnf = CNF()
    cnf.from_file(args.path, compressed_with='lzma')
    print("NVars: {}".format(cnf.nv))
    g.add_vertex(cnf.nv + 1)
    w = g.new_edge_property("float")
    for clause in cnf:
        for tup in itertools.combinations(clause, 2):
            e = g.edge(g.vertex(abs(tup[0])), g.vertex(abs(tup[1])), add_missing=True)
            w[e] = w[e] + 1.0 / pow(2, len(clause))
    pos = sfdp_layout(g, eweight=w)
    graph_draw(g, pos, output_size=(1000, 1000), vertex_color=[1,1,1,0],
           vertex_size=1, edge_pen_width=1.2, output="graph.pdf")