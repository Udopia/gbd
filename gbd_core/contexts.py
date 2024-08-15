
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

### Default Context
default = "cnf"

### Configuration of Available Contexts
config = {
    "cnf" : {
        "description": "Conjunctive Normal Form (CNF) in DIMACS format",
        "suffix": ".cnf",
        "idfunc": cnf_hash,
    },
    "sancnf" : {
        "description": "Sanitized Conjunctive Normal Form (CNF) in DIMACS format",
        "suffix": ".sanitized.cnf",
        "idfunc": cnf_hash,
    },
    "kis" : {
        "description": "k-Independent Set (KIS) in DIMACS-like graph format",
        "suffix": ".kis",
        "idfunc": cnf_hash,
    },
    "opb" : {
        "description": "Pseudo-Boolean Optimization Problem in OPB format",
        "suffix": ".opb",
        "idfunc": opb_hash,
    },
    "wecnf" : {
        "description": "Weighted Extended Conjunctive Normal Form (WECNF)",
        "suffix": ".wecnf",
        "idfunc": cnf_hash,
    },
    "wcnf"  : {
        "description": "MaxSAT instances in WCNF format",
        "suffix": ".wcnf",
        "idfunc": wcnf_hash,
    }
}

def description(context):
    return config[context]['description']

def suffixes(context):
    packed = [ "", ".gz", ".lzma", ".xz", ".bz2" ]
    return [ config[context]['suffix'] + p for p in packed ]

def idfunc(context):
    return config[context]['idfunc']

def contexts():
    return config.keys()

def default_context():
    return default

def get_context_by_suffix(benchmark):
    for context in contexts():
        for suffix in suffixes(context):
            if benchmark.endswith(suffix):
                return context
    return None

def identify(path, ct=None):
    context = ct or get_context_by_suffix(path)    
    if context is None:
        raise Exception("Unable to associate context: " + path)
    else:
        idf = idfunc(context)
        return idf(path)
