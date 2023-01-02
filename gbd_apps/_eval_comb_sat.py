# GBD Benchmark Database (GBD)

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

from pysat.formula import CNF
from pysat.solvers import Lingeling
from pysat.solvers import Cadical

import cscl.dimacs_printer as dimacs
from cscl.cardinality_constraint_encoders import encode_at_most_k_constraint_ltseq, encode_exactly_k_constraint

# bitvector width
_BW = 32

def optimal_comb(api: GBD, args):
    df = api.query(args.query, [], args.runtimes)
    df[args.runtimes] = df[args.runtimes][df.applymap(util.is_number)].applymap(float)
    df.fillna(2*args.tlim, inplace=True)

    cnf = dimacs.DIMACSPrinter()
    _ACT = [cnf.create_literal() for _ in range(0, len(args.runtimes))]
    _MAX = get_bitvector(cnf, int(pow(2, _BW-1)-1))
    for c in encode_at_most_k_constraint_ltseq(cnf, args.size, _ACT):
        cnf.consume_clause(c)

    # encode row-wise minima
    MINS = []
    for row in result:
        i = 0
        B0_ = get_bitvector(cnf, int(row[0]))
        B0 = if_then_else(cnf, B0_, _MAX, _ACT[i])
        for rt in row[1:]:
            i = i + 1
            B1_ = get_bitvector(cnf, int(rt))
            B1 = if_then_else(cnf, B1_, _MAX, _ACT[i])
            Bcarry = get_carry_bits(cnf, B1, [-i for i in B0])
            Bmin = if_then_else(cnf, B0, B1, Bcarry[_BW-1])
            B0 = Bmin
        MINS.append(B0) # B0 is now minimum of row

    # encode sum of minima
    A = MINS[0]
    for B in MINS[1:]:
        SUM = get_sum_bits(cnf, A, B)
        A = SUM
    
    solver = Cadical(bootstrap_with=cnf.clauses, with_proof=False)
    result = solver.solve()
    if result == True:
        model = solver.get_model()
        print(slice_model(model, _ACT))
        print(decode_bitvector(slice_model(model, A)))
        #Bcarry = get_carry_bits(cnf, B1, [-i for i in B0])


def slice_model(model, A):
    return model[A[0]-1:A[len(A)-1]]

# DECODE n from bitvector A
def decode_bitvector(D):
    i = 0
    n = 0
    for b in D:
        if b > 0:
            n = n + pow(2, i)
        i = i + 1
    return n

# ENCODE n to bitvector A
def get_bitvector(cnf, n):
    A = [cnf.create_literal() for _ in range(0, _BW)]
    for i in range(0, len(A)):
        if n & 1: 
            cnf.consume_clause((A[i],))
        else: 
            cnf.consume_clause((-A[i],))
        n = n >> 1
    return A

# CALCULATE D <=> (carry ? A : B):
def if_then_else(cnf, A, B, carry):
    # encode: d_i <-> [-c or a_i] and [c or b_i]
    # that makes (->): (-d_i or -c or a_i) and (-d_i or c or b_i)
    # and (<-): (d_i or -b_i or c) and (d_i or -a_i or -c)
    D = [cnf.create_literal() for _ in range(0, _BW)]
    for i in range(0, len(A)):
        cnf.consume_clause((-carry, -D[i], A[i]))
        cnf.consume_clause((-carry, D[i], -A[i]))
        cnf.consume_clause((carry, -D[i], B[i]))
        cnf.consume_clause((carry, D[i], -B[i]))
    return D

# CALCULATE VECTOR OF CARRY BITS (C)
# A, B, C are bit-vectors of width b, dimacs_out is a clause-consumer
# C are constrained to be the carry bits of the sum of A and B
#
# C[i] = (A[i] and B[i]) or [(A[i] xor B[i]) and C[i-1]]
#
# (C denotes C[i-1]):
# A B C | C[i] | -C[i]
# 0 0 0 |   0  |    1
# 1 0 0 |   0  |    1
# 0 1 0 |   0  |    1
# 1 1 0 |   1  |    0
# 0 0 1 |   0  |    1
# 1 0 1 |   1  |    0
# 0 1 1 |   1  |    0
# 1 1 1 |   1  |    0
#
# C[i] = (B C) (A C) (A B)
#-C[i] = (-B -C) (-A -C) (-A -B)
#
def get_carry_bits(cnf, A, B):
    C = [cnf.create_literal() for _ in range(0, _BW)]
    # Encode: C[i] -> (B C[i-1]) (A C[i-1]) (A B)
    cnf.consume_clause((-C[0], A[0]))
    cnf.consume_clause((-C[0], B[0]))
    for i in range(1, _BW):
        cnf.consume_clause((-C[i], B[i], C[i-1]))
        cnf.consume_clause((-C[i], A[i], C[i-1]))
        cnf.consume_clause((-C[i], A[i], B[i]))
    # Encode: C[i] <- (B C[i-1]) (A C[i-1]) (A B)
    cnf.consume_clause((C[0], -A[0], -B[0]))
    for i in range(1, _BW):
        cnf.consume_clause((C[i], -B[i], -C[i-1]))
        cnf.consume_clause((C[i], -A[i], -C[i-1]))
        cnf.consume_clause((C[i], -A[i], -B[i]))
    return C


def get_sum_bits(cnf, A, B):
    S = [cnf.create_literal() for _ in range(0, _BW)]
    C = get_carry_bits(cnf, A, B)
    cnf.consume_clause((-S[0], A[0], B[0]))
    cnf.consume_clause((-S[0], -A[0], -B[0]))
    cnf.consume_clause((S[0], A[0], -B[0]))
    cnf.consume_clause((S[0], -A[0], B[0]))
    for i in range(1, _BW):
        # 0 + 0 + 0 -> 0
        cnf.consume_clause((A[i], B[i], C[i-1], -S[i]))
        # 1 + 1 + 0 -> 0
        cnf.consume_clause((-A[i], -B[i], C[i-1], -S[i]))
        cnf.consume_clause((A[i], -B[i], -C[i-1], -S[i]))
        cnf.consume_clause((-A[i], B[i], -C[i-1], -S[i]))
        # 1 + 0 + 0 -> 1
        cnf.consume_clause((A[i], -B[i], C[i-1], S[i]))
        cnf.consume_clause((-A[i], B[i], C[i-1], S[i]))
        cnf.consume_clause((A[i], B[i], -C[i-1], S[i]))
        # 1 + 1 + 1 -> 1
        cnf.consume_clause((-A[i], -B[i], -C[i-1], S[i]))
    return S