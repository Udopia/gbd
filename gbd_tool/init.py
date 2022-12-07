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
import lzma
import shutil
import sys
import atexit

import multiprocessing
import pebble
from concurrent.futures import as_completed
from iteration_utilities import grouper

from gbd_tool import contexts, util
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

try:
    from gbdc import sanitize
except ImportError:
    def sanitize(path) -> bool:
        raise GBDException(METHOD_UNAVAILABLE.format("sanitize"))


# Initialize table 'local' with instances found under given path
def init_local(api: GBD, root):
    clocal = contexts.prepend_context("local", api.context)
    api.database.create_feature(clocal, permissive=True)
    paths = set(res[0] for res in api.query_search(group_by=clocal))
    missing_files = [path for path in paths if not isfile(path)]
    if len(missing_files) and confirm("{} files not found. Remove stale entries from local table?".format(len(missing_files))):
        for paths_chunk in slice_iterator(missing_files, 1000):
            api.database.delete_values(clocal, paths_chunk)
    resultset = []
    #if clocal in api.get_features():
    if api.context == "cnf2":
        api.database.create_feature("cnf_to_cnf2", permissive=True)
        api.database.create_feature("cnf2_to_cnf", permissive=True)
        for [hash, local] in api.query_search(resolve=["local"]):
            if local:
                missing = [ p for p in local.split(",") if not p in paths ]
                if len(missing):
                    resultset.append([hash, ",".join(missing)])
    else:
        for suffix in contexts.suffix_list(api.context):
            for path in glob.iglob(root + "/**/*" + suffix, recursive=True):
                #if not len(api.query_search("{}='{}'".format(clocal, path))):
                if not path in paths:
                    resultset.append(("", path))
    run(api, resultset, compute_hash, {'context': api.context, **api.get_limits()})


def compute_hash(buggyhash, path, args):
    if path:
        eprint('Hashing {}'.format(path))
        if args["context"] == "cnf2":
            paths=path.split(",")
            hashvalue = gbd_hash(paths[0])
            clocal = contexts.prepend_context("local", args["context"])
            return [ (clocal, hashvalue, p) for p in paths ] + [ ("cnf_to_cnf2", buggyhash, hashvalue), ("cnf2_to_cnf", hashvalue, buggyhash) ]
        else:
            hashvalue = gbd_hash(path)
            clocal = contexts.prepend_context("local", args["context"])
            return [ (clocal, hashvalue, path) ]
    else:
        eprint("Entry not found: " + buggyhash)
        return []


def safe_run_results(api: GBD, result, check=False):
    for attr in result:
        name, hashv, value = attr[0], attr[1], attr[2]
        eprint("Saving {}={} for {}".format(name, value, hashv))
        if check and not name in api.database.get_features():
            if name.endswith("_local"):
                api.database.create_feature(name)
            else:
                api.database.create_feature(name, "empty")
        api.database.set_values(name, value, [hashv])


# Sanitize given instances
def init_sani(api: GBD, query, hashes):
    api.database.create_feature("sancnf_local", permissive=True)
    api.database.create_feature("cnf_to_sancnf", permissive=True)
    api.database.create_feature("sancnf_to_cnf", permissive=True)

    resultset = []

    for [hashv, local] in api.query_search(query, hashes, ["local"]):
        if local:
            missing = []
            for path in local.split(","):
                sanname = os.path.splitext(path)[0]
                if not isfile(sanname):
                    missing.append(path)
            if len(missing):
                resultset.append([hashv, ",".join(missing)])
        else:
            eprint("Entry not found: " + hashv)
            return []

    run(api, resultset, compute_sani, api.get_limits())

def compute_sani(hashvalue, paths, args):
    result = []

    sanname = None
    sanhash = None
    for path in paths.split(","):
        eprint('Sanitizing {}'.format(path))

        if sanname == None or sanhash == None:
            sanname = os.path.splitext(path)[0]
            #with lzma.open(sanname, 'w') as f, stdout_redirected(f):
            with open(sanname, 'w') as f, util.stdout_redirected(f):
                #atexit.register(os.remove, sanname)
                if sanitize(path): 
                    sanhash = gbd_hash(sanname)
                    result.extend([ ('sancnf_local', sanhash, sanname), ('sancnf_to_cnf', sanhash, hashvalue), ('cnf_to_sancnf', hashvalue, sanhash) ])
                #atexit.unregister(os.remove)
        else:
            sanname2 = os.path.splitext(path)[0]
            shutil.copy(sanname, sanname2)
            result.append(('sancnf_local', sanhash, sanname2))

    return result


# Parallel Runner
def run(api: GBD, resultset, func, args: dict):
    first = True
    if api.jobs == 1:
        for (hash, local) in resultset:
            result = func(hash, local, args)
            safe_run_results(api, result, check=first)
            first = False
    else:
        njobs=min(multiprocessing.cpu_count(), api.jobs)
        #for subset in grouper(resultset, njobs):
        with pebble.ProcessPool(max_workers=njobs, max_tasks=1) as p:
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
    kispath = reduce(lambda path, suffix: path[:-len(suffix)] if path.endswith(suffix) else path, contexts.suffix_list('cnf'), cnfpath)
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