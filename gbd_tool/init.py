# GBD Benchmark Database (GBD)
# Copyright (C) 2021 Markus Iser, Karlsruhe Institute of Technology (KIT)
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


import os
from os.path import isfile, basename, exists

from functools import reduce

import glob

import hashlib

import multiprocessing
import pebble
from concurrent.futures import as_completed

from gbd_tool import config, util
from gbd_tool.gbd_api import GBD, GBDException
from gbd_tool.gbd_hash import gbd_hash
from gbd_tool.util import eprint, confirm, open_cnf_file, slice_iterator

METHOD_UNAVAILABLE = "Method '{}' not available. Install gdbc module from: https://github.com/sat-clique/cnftools"

try:
    from gbdc import extract_base_features
except ImportError:
    def extract_base_features(path, tlim, mlim) -> dict:
        raise GBDException(METHOD_UNAVAILABLE.format("extract_base_features"))

try:
    from gbdc import extract_gate_features
except ImportError:
    def extract_gate_features(path, tlim, mlim) -> dict:
        raise GBDException(METHOD_UNAVAILABLE.format("extract_gate_features"))

try:
    from gbdc import cnf2kis
except ImportError:
    def cnf2kis(in_path, out_path, max_edges, max_nodes, tlim, mlim, flim) -> dict:
        raise GBDException(METHOD_UNAVAILABLE.format("cnf2kis"))

try:
    from gbdc import isohash
except ImportError:
    def isohash(path) -> dict:
        raise GBDException(METHOD_UNAVAILABLE.format("isohash"))


# Initialize table 'local' with instances found under given path
def init_local(api: GBD, root):
    clocal=util.prepend_context("local", api.context)
    api.database.create_feature(clocal, permissive=True)
    sanitize = [path[0] for path in api.query_search(group_by=clocal) if not isfile(path[0])]
    if len(sanitize) and confirm("{} files not found. Remove stale entries from local table?".format(len(sanitize))):
        for paths in slice_iterator(sanitize, 1000):
            api.database.delete_values("local", paths)
    resultset = []
    #if clocal in api.get_features():
    for suffix in config.suffix_list(api.context):
        for path in glob.iglob(root + "/**/*" + suffix, recursive=True):
            if not len(api.query_search("{}='{}'".format(clocal, path))):
                resultset.append(("", path))
    run(api, resultset, compute_hash, api.get_limits())


def compute_hash(nohashvalue, path, args):
    eprint('Hashing {}'.format(path))
    hashvalue = gbd_hash(path)
    return [ ('local', hashvalue, path) ]


def safe_run_results(api: GBD, result, check=False):
    for attr in result:
        name, hashv, value = attr[0], attr[1], attr[2]
        eprint("Saving {}={} for {}".format(name, value, hashv))
        if check and not name in api.database.get_features():
            api.database.create_feature(name, "empty")
        api.database.set_values(name, value, [hashv])


# Parallel Runner
def run(api: GBD, resultset, func, args: dict):
    first = True
    if api.jobs == 1:
        for (hash, local) in resultset:
            result = func(hash, local, args)
            safe_run_results(api, result, check=first)
            first = False
    else:
        with pebble.ProcessPool(min(multiprocessing.cpu_count(), api.jobs)) as p:
            futures = [ p.schedule(func, (hash, local, args)) for (hash, local) in resultset ]
            for f in as_completed(futures):  #, timeout=api.tlim if api.tlim > 0 else None):
                try:
                    result = f.result()
                    safe_run_results(api, result, check=first)
                    first = False
                except pebble.ProcessExpired as e:
                    f.cancel()
                    eprint("{}: {}".format(e.__class__.__name__, e))
                except GBDException as e:  # might receive special handling in the future
                    eprint("{}: {}".format(e.__class__.__name__, e))
                except Exception as e:
                    eprint("{}: {}".format(e.__class__.__name__, e))


def init_transform_cnf_to_kis(api: GBD, query, hashes, max_edges, max_nodes):
    api.database.create_feature('kis_local', permissive=True)
    api.database.create_feature('kis_nodes', "empty", permissive=True)
    api.database.create_feature('kis_edges', "empty", permissive=True)
    api.database.create_feature('kis_k', "empty", permissive=True)
    api.database.create_feature('cnf_to_kis', permissive=True)
    api.database.create_feature('kis_to_cnf', permissive=True)
    resultset = api.query_search(query, hashes, ["local"], collapse="MIN")
    run(api, resultset, transform_cnf_to_kis, { **api.get_limits(), 'max_edges': max_edges, 'max_nodes': max_nodes })

def transform_cnf_to_kis(cnfhash, cnfpath, args):
    if not cnfhash or not cnfpath:
        raise GBDException("Arguments missing: transform_cnf_to_kis({}, {})".format(cnfhash, cnfpath))
    kispath = reduce(lambda path, suffix: path[:-len(suffix)] if path.endswith(suffix) else path, config.suffix_list('cnf'), cnfpath)
    kispath = kispath + ".kis"

    if isfile(kispath):
        raise GBDException("{} already exists. Aborting.".format(basename(kispath)))

    eprint('Transforming {} to k-ISP {}'.format(cnfpath, kispath))
    result = cnf2kis(cnfpath, kispath, args['max_edges'], args['max_nodes'], args['tlim'], args['mlim'], args['flim'])

    if not "local" in result:
        if exists(kispath):
            os.path.remove(kispath)
        eprint('''{} got {}. Aborting.'''.format(basename(kispath), result['hash']))
        return [ ('cnf_to_kis', cnfhash, result['hash']), ('kis_to_cnf', result['hash'], cnfhash) ]

    return [ ('kis_local', result['hash'], result['local']),
            ('kis_nodes', result['hash'], result['nodes']), 
            ('kis_edges', result['hash'], result['edges']), 
            ('kis_k', result['hash'], result['k']), 
            ('cnf_to_kis', cnfhash, result['hash']), 
            ('kis_to_cnf', result['hash'], cnfhash) ]


# Initialize base feature tables for given instances
def init_base_features(api: GBD, query, hashes):
    resultset = api.query_search(query, hashes, ["local"], collapse="MIN")
    run(api, resultset, base_features, api.get_limits())

def base_features(hashvalue, filename, args):
    eprint('Extracting base features from {} {}'.format(hashvalue, filename))
    rec = extract_base_features(filename, args['tlim'], args['mlim'])
    return [ (key, hashvalue, int(value) if isinstance(value, float) and value.is_integer() else value) for key, value in rec.items() ]


# Initialize gate feature tables for given instances
def init_gate_features(api: GBD, query, hashes):
    resultset = api.query_search(query, hashes, ["local"], collapse="MIN")
    run(api, resultset, gate_features, api.get_limits())

def gate_features(hashvalue, filename, args):
    eprint('Extracting gate features from {} {}'.format(hashvalue, filename))
    rec = extract_gate_features(filename, args['tlim'], args['mlim'])
    return [ (key, hashvalue, int(value) if isinstance(value, float) and value.is_integer() else value) for key, value in rec.items() ]


# Initialize Graph Features known from Network Analysis
def init_networkit_features(api: GBD, query, hashes):
    try:
        import networkit as nk
    except ImportError as e:
        raise GBDException("Module 'networkit' not found. Setup https://networkit.github.io/")
    nk.setNumberOfThreads(min(multiprocessing.cpu_count(), api.jobs))
    resultset = api.query_search(query, hashes, ["local"], collapse="MIN")
    for (hash, local) in resultset: 
        result = networkit_features(hash, local, {})
        eprint(result['hashvalue'])
        for att in result['attributes']:
            eprint(att[1] + "=" + att["2"])

def networkit_features(hashvalue, filename, args):
    rec = dict()
    # TODO: Calculate Networkit Features
    return [ (key, hashvalue, int(value) if isinstance(value, float) and value.is_integer() else value) for key, value in rec.items() ]


# Initialize degree_sequence_hash for given instances
def init_iso_hash(api: GBD, query, hashes):
    if not api.feature_exists("isohash"):
        api.create_feature("isohash", "empty")
    resultset = api.query_search(query, hashes, ["local"], collapse="MIN")
    run(api, resultset, compute_iso_hash, api.get_limits())

def compute_iso_hash(hashvalue, filename, args):
    eprint('Computing iso hash for {}'.format(filename))
    isoh = isohash(filename)
    return [ ('isohash', hashvalue, isoh) ]