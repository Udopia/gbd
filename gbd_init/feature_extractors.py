
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

import pandas as pd
import os
import glob

from gbd_core.contexts import suffix_list, identify, get_context_by_suffix
from gbd_core.api import GBD, GBDException
from gbd_core.util import eprint, confirm
from gbd_init.initializer import Initializer, InitializerException

try:
    from gbdc import extract_base_features, base_feature_names, extract_gate_features, gate_feature_names, isohash, wcnfisohash, wcnf_base_feature_names, extract_wcnf_base_features, opb_base_feature_names, extract_opb_base_features
except ImportError:
    def extract_base_features(path, tlim, mlim):
        return [ ]
    
    def base_feature_names():
        return [ ]

    def extract_gate_features(path, tlim, mlim):
        return [ ]

    def gate_feature_names():
        return [ ]
    
    def isohash(path):
        return [ ]

    def extract_wcnf_base_features(path, tlim, mlim):
        return [ ]
    
    def wcnf_base_feature_names():
        return [ ]

    def extract_opb_base_features(path, tlim, mlim):
        return [ ]

    def opb_base_feature_names():
        return [ ]


## GBDHash
def compute_hash(hash, path, limits):
    eprint('Hashing {}'.format(path))
    hash = identify(path)
    return [ ("local", hash, path), ("filename", hash, os.path.basename(path)) ]

## ISOHash
def compute_isohash(hash, path, limits):
    eprint('Computing ISOHash for {}'.format(path))
    context = get_context_by_suffix(path)
    if context == 'wcnf':
        ihash = wcnfisohash(path)
    else:
        ihash = isohash(path)
    return [ ('isohash', hash, ihash) ]

## Base Features
def compute_base_features(hash, path, limits):
    eprint('Extracting base features from {} {}'.format(hash, path))
    rec = extract_base_features(path, limits['tlim'], limits['mlim'])
    return [ (key, hash, int(value) if isinstance(value, float) and value.is_integer() else value) for key, value in rec.items() ]

## Gate Features
def compute_gate_features(hash, path, limits):
    eprint('Extracting gate features from {} {}'.format(hash, path))
    rec = extract_gate_features(path, limits['tlim'], limits['mlim'])
    return [ (key, hash, int(value) if isinstance(value, float) and value.is_integer() else value) for key, value in rec.items() ]   

## WCNF Base Features
def compute_wcnf_base_features(hash, path, limits):
    eprint('Extracting WCNF base features from {} {}'.format(hash, path))
    rec = extract_wcnf_base_features(path, limits['tlim'], limits['mlim'])
    return [ (key, hash, int(value) if isinstance(value, float) and value.is_integer() else value) for key, value in rec.items() ]

## OPB Base Features
def compute_opb_base_features(hash, path, limits):
    eprint('Extracting OPB base features from {} {}'.format(hash, path))
    rec = extract_opb_base_features(path, limits['tlim'], limits['mlim'])
    return [ (key, hash, int(value) if isinstance(value, float) and value.is_integer() else value) for key, value in rec.items() ]


generic_extractors = {
    "base" : {
        "contexts" : [ "cnf" ],
        "features" : [ (name, "empty") for name in base_feature_names() ],
        "compute" : compute_base_features,
    },
    "gate" : {
        "contexts" : [ "cnf" ],
        "features" : [ (name, "empty") for name in gate_feature_names() ],
        "compute" : compute_gate_features,
    },
    "isohash" : {
        "contexts" : [ "cnf", "wcnf" ],
        "features" : [ ("isohash", "empty") ],
        "compute" : compute_isohash,
    },
    "wcnfbase" : {
        "contexts" : [ "wcnf" ],
        "features" : [ (name, "empty") for name in wcnf_base_feature_names() ],
        "compute" : compute_wcnf_base_features,
    },
    "opbbase" : {
        "contexts" : [ "opb" ],
        "features" : [ (name, "empty") for name in opb_base_feature_names() ],
        "compute" : compute_opb_base_features,
    }
}


def init_features_generic(key: str, api: GBD, rlimits, df, target_db):
    einfo = generic_extractors[key]
    context = api.database.dcontext(target_db)
    if not context in einfo["contexts"]:
        raise InitializerException("Target database context must be in {}".format(einfo["contexts"]))
    extractor = Initializer(api, rlimits, target_db, einfo["features"], einfo["compute"])
    extractor.create_features()
    extractor.run(df)


def init_local(api: GBD, rlimits, root, target_db):
    context = api.database.dcontext(target_db)
    
    features = [ ("local", None), ("filename", None) ]
    extractor = Initializer(api, rlimits, target_db, features, compute_hash)
    extractor.create_features()

    # Cleanup stale entries
    df = api.query(group_by=context + ":local")
    dfilter = df["local"].apply(lambda x: not x or not os.path.isfile(x))
    missing = df[dfilter]
    if len(missing) and api.verbose:
        for path in missing["local"].tolist():
            eprint(path)
    if len(missing) and confirm("{} files not found. Remove stale entries from local table?".format(len(missing))):
        api.reset_values("local", values=missing["local"].tolist())

    # Create df with paths not yet in local table
    paths = [ path for suffix in suffix_list(context) for path in glob.iglob(root + "/**/*" + suffix, recursive=True) ]
    df2 = pd.DataFrame([(None, path) for path in paths if not path in df["local"].to_list()], columns=["hash", "local"])
    
    extractor.run(df2)