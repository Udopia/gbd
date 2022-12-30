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

import pandas as pd
import os
import glob

from gbd_core.api import GBD, GBDException
from gbd_core.util import eprint, confirm
from gbd_init.initializer import Initializer
from gbd_init.gbdhash import gbd_hash

from gbdc import extract_base_features, extract_gate_features, isohash


## GBDHash
def compute_hash(self, hash, path, limits):
    eprint('Hashing {}'.format(path))
    hash = gbd_hash(path)
    return [ ("local", hash, path), ("filename", hash, os.path.basename(path)) ]

def init_local(api: GBD, root, target_db):
    contexts = [ 'cnf', 'sancnf' ]
    features = [ ("local", None), ("filename", None) ]
    extractor = Initializer(contexts, contexts, api, target_db, features, compute_hash)
    extractor.create_features()

    df = api.query(group_by="local")
    
    dfilter = df["local"].apply(lambda x: not x or not os.path.isfile(x))
    missing = df[dfilter]
    if len(missing) and confirm("{} files not found. Remove stale entries from local table?".format(len(missing))):
        api.remove_attributes("local", values=missing["local"].tolist())

    paths = [ path for suffix in contexts.suffix_list(api.context) for path in glob.iglob(root + "/**/*" + suffix, recursive=True) ]
    df2 = pd.DataFrame([(None, path) for path in paths if not path in df["local"].to_list()], columns=["hash", "local"])
    
    extractor.run(df2)


## ISOHash
def compute_isohash(self, hash, path, limits):
    eprint('Computing ISOHash for {}'.format(path))
    ihash = isohash(path)
    return [ ('isohash', hash, ihash) ]

# Initialize degree_sequence_hash for given instances
def init_isohash(api: GBD, query, hashes, target_db):
    contexts = [ 'cnf', 'sancnf' ]
    features = [ ('isohash', 'empty') ]
    extractor = Initializer(contexts, contexts, api, target_db, features, compute_isohash)
    extractor.create_features()
    df = api.query(query, hashes, ["local"], collapse="MIN")
    extractor.run(df)


## Base Features
def compute_base_features(self, hash, path, limits):
    eprint('Extracting base features from {} {}'.format(hash, path))
    rec = extract_base_features(path, limits['tlim'], limits['mlim'])
    return [ (key, hash, int(value) if isinstance(value, float) and value.is_integer() else value) for key, value in rec.items() ]

def init_base_features(api: GBD, query, hashes, target_db):
    contexts = [ 'cnf', 'sancnf' ]
    features = [ ("base_features_runtime", "empty"), ("clauses", "empty"), ("variables", "empty"), ("clause_size_1", "empty"), ("clause_size_2", "empty"), ("clause_size_3", "empty"), 
        ("clause_size_4", "empty"), ("clause_size_5", "empty"), ("clause_size_6", "empty"), ("clause_size_7", "empty"), ("clause_size_8", "empty"), ("clause_size_9", "empty"), 
        ("horn_clauses", "empty"), ("inv_horn_clauses", "empty"), ("positive_clauses", "empty"), ("negative_clauses", "empty"),
        ("horn_vars_mean", "empty"), ("horn_vars_variance", "empty"), ("horn_vars_min", "empty"), ("horn_vars_max", "empty"), ("horn_vars_entropy", "empty"),
        ("inv_horn_vars_mean", "empty"), ("inv_horn_vars_variance", "empty"), ("inv_horn_vars_min", "empty"), ("inv_horn_vars_max", "empty"), ("inv_horn_vars_entropy", "empty"),
        ("vg_degrees_mean", "empty"), ("vg_degrees_variance", "empty"), ("vg_degrees_min", "empty"), ("vg_degrees_max", "empty"), ("vg_degrees_entropy", "empty"),
        ("balance_clause_mean", "empty"), ("balance_clause_variance", "empty"), ("balance_clause_min", "empty"), ("balance_clause_max", "empty"), ("balance_clause_entropy", "empty"),
        ("balance_vars_mean", "empty"), ("balance_vars_variance", "empty"), ("balance_vars_min", "empty"), ("balance_vars_max", "empty"), ("balance_vars_entropy", "empty"),
        ("vcg_vdegrees_mean", "empty"), ("vcg_vdegrees_variance", "empty"), ("vcg_vdegrees_min", "empty"), ("vcg_vdegrees_max", "empty"), ("vcg_vdegrees_entropy", "empty"),
        ("vcg_cdegrees_mean", "empty"), ("vcg_cdegrees_variance", "empty"), ("vcg_cdegrees_min", "empty"), ("vcg_cdegrees_max", "empty"), ("vcg_cdegrees_entropy", "empty"),
        ("cg_degrees_mean", "empty"), ("cg_degrees_variance", "empty"), ("cg_degrees_min", "empty"), ("cg_degrees_max", "empty"), ("cg_degrees_entropy", "empty") ]
    extractor = Initializer(contexts, contexts, api, target_db, features, compute_base_features)
    extractor.create_features()
    df = api.query(query, hashes, ["local"], collapse="MIN")
    extractor.run(df)


## Gate Features
def compute_gate_features(self, hash, path, limits):
    eprint('Extracting gate features from {} {}'.format(hash, path))
    rec = extract_gate_features(path, limits['tlim'], limits['mlim'])
    return [ (key, hash, int(value) if isinstance(value, float) and value.is_integer() else value) for key, value in rec.items() ]

def init_gate_features(api: GBD, query, hashes, target_db):
    contexts = [ 'cnf', 'sancnf' ]
    features = [ ("gate_features_runtime", "empty"), ("n_vars", "empty"), ("n_gates", "empty"), ("n_roots", "empty"),
        ("n_none", "empty"), ("n_generic", "empty"), ("n_mono", "empty"), ("n_and", "empty"), ("n_or", "empty"), ("n_triv", "empty"), ("n_equiv", "empty"), ("n_full", "empty"),
        ("levels_mean", "empty"), ("levels_variance", "empty"), ("levels_min", "empty"), ("levels_max", "empty"), ("levels_entropy", "empty"),
        ("levels_none_mean", "empty"), ("levels_none_variance", "empty"), ("levels_none_min", "empty"), ("levels_none_max", "empty"), ("levels_none_entropy", "empty"),
        ("levels_generic_mean", "empty"), ("levels_generic_variance", "empty"), ("levels_generic_min", "empty"), ("levels_generic_max", "empty"), ("levels_generic_entropy", "empty"),
        ("levels_mono_mean", "empty"), ("levels_mono_variance", "empty"), ("levels_mono_min", "empty"), ("levels_mono_max", "empty"), ("levels_mono_entropy", "empty"),
        ("levels_and_mean", "empty"), ("levels_and_variance", "empty"), ("levels_and_min", "empty"), ("levels_and_max", "empty"), ("levels_and_entropy", "empty"),
        ("levels_or_mean", "empty"), ("levels_or_variance", "empty"), ("levels_or_min", "empty"), ("levels_or_max", "empty"), ("levels_or_entropy", "empty"),
        ("levels_triv_mean", "empty"), ("levels_triv_variance", "empty"), ("levels_triv_min", "empty"), ("levels_triv_max", "empty"), ("levels_triv_entropy", "empty"),
        ("levels_equiv_mean", "empty"), ("levels_equiv_variance", "empty"), ("levels_equiv_min", "empty"), ("levels_equiv_max", "empty"), ("levels_equiv_entropy", "empty"),
        ("levels_full_mean", "empty"), ("levels_full_variance", "empty"), ("levels_full_min", "empty"), ("levels_full_max", "empty"), ("levels_full_entropy", "empty") ]
    extractor = Initializer(contexts, contexts, api, target_db, features, compute_gate_features)
    extractor.create_features()
    df = api.query(query, hashes, ["local"], collapse="MIN")
    extractor.run(df)