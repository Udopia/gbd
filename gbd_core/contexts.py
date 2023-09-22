
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

from gbd_init.gbdhash import cnf_hash, opb_hash, wcnf_hash


packed = [ "", ".gz", ".lzma", ".xz", ".bz2" ]

context_data = {
    "cnf" : {
        "id" : 100,
        "description" : "Conjunctive Normal Form (CNF) in DIMACS format",
        "suffixes" : [ ".cnf" + p for p in packed ],
        "hash": cnf_hash,
    },
    "sancnf" : {
        "id" : 101,
        "description" : "Sanitized Conjunctive Normal Form (CNF) in DIMACS format",
        "suffixes" : [ ".sanitized.cnf" + p for p in packed ],
        "hash": cnf_hash,
    },
    "kis" : {
        "id" : 200,
        "description" : "k-Independent Set (KIS) in DIMACS-like graph format",
        "suffixes" : [ ".kis" + p for p in packed ],
        "hash": cnf_hash,
    },
    "opb" : {
        "id" : 300,
        "description" : "Pseudo-Boolean Optimization Problem in OPB format",
        "suffixes" : [ ".opb" + p for p in packed ],
        "hash": opb_hash,
    },
    "wecnf" : {
        "id" : 400,
        "description" : "Weighted Extended Conjunctive Normal Form (WECNF)",
        "suffixes" : [ ".wecnf" + p for p in packed ],
        "hash": cnf_hash,
    },
    "wcnf"  : {
        "id" : 500,
        "description" : "MaxSAT instances in WCNF format",
        "suffixes" : [ ".wcnf" + p for p in packed ],
        "hash": wcnf_hash,
    }
}

def suffix_list(context):
    return context_data[context]['suffixes']

def contexts():
    return context_data.keys()

def get_context_by_suffix(benchmark):
    for context in contexts():
        for suffix in suffix_list(context):
            if benchmark.endswith(suffix):
                return context
    return None
    

def identify(path, ct=None):
    context = ct or get_context_by_suffix(path)    
    if context is None:
        raise Exception("Unable to associate context: " + path)
    else:
        idfunc = context_data[context]['hash']
        return idfunc(path)
