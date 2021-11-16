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

from concurrent.futures.process import BrokenProcessPool
import multiprocessing
from multiprocessing import Pool

import os
from os.path import isfile

import hashlib
import csv

from concurrent.futures import ProcessPoolExecutor, wait, FIRST_EXCEPTION, as_completed, CancelledError, TimeoutError

import networkit as nk

from gbd_tool import config, util
from gbd_tool.gbd_api import GBD, GBDException
from gbd_tool.gbd_hash import gbd_hash
from gbd_tool.util import eprint, confirm, open_cnf_file

#import faulthandler
#faulthandler.enable()

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
    from gbdc import transform_cnf_to_kis
except ImportError:
    def transform_cnf_to_kis(path, tlim, mlim) -> dict:
        raise GBDException(METHOD_UNAVAILABLE.format("transform_cnf_to_kisses"))


# Initialize table 'local' with instances found under given path
def init_local(api: GBD, path):
    eprint('Initializing local path entries {} using {} cores'.format(path, api.jobs))
    if api.jobs == 1 and multiprocessing.cpu_count() > 1:
        eprint("Activate parallel initialization using --jobs={}".format(multiprocessing.cpu_count()))
    remove_stale_benchmarks(api)
    init_benchmarks(api, path)

def slice_iterator(data, slice_len):
    it = iter(data)
    while True:
        items = []
        for index in range(slice_len):
            try:
                item = next(it)
            except StopIteration:
                if items == []:
                    return # we are done
                else:
                    break # exits the "for" loop
            items.append(item)
        yield items

def remove_stale_benchmarks(api: GBD):
    eprint("Sanitizing local path entries ... ")
    feature=util.prepend_context("local", api.context)
    paths = [path[0] for path in api.query_search(group_by=feature)]
    sanitize = list(filter(lambda path: not isfile(path), paths))
    if len(sanitize) and confirm("{} files not found. Remove stale entries from local table?".format(len(sanitize))):
        for paths in slice_iterator(sanitize, 100):
            api.database.delete_values("local", paths)

def compute_hash(nohashvalue, path):
    eprint('Hashing {}'.format(path))
    hashvalue = gbd_hash(path)
    return [ ('local', path, hashvalue) ]

def init_benchmarks(api: GBD, root):
    resultset = []
    for root, dirnames, filenames in os.walk(root):
        for filename in filenames:
            path = os.path.join(root, filename)
            if any(path.endswith(suffix) for suffix in config.suffix_list(api.context)):
                feature=util.prepend_context("local", api.context)
                hashes = api.query_search("{}='{}'".format(feature, path))
                if len(hashes) != 0:
                    eprint('Problem {} already hashed'.format(path))
                else:
                    resultset.append(("", path))
    run(api, resultset, compute_hash)


# Parallel Runner
def run(api: GBD, resultset, func, tlim = 0, mlim = 0):
    if api.jobs == 1:
        for (hash, local) in resultset:
            api.set_attributes_locked(func(hash, local, tlim, mlim))
    else:
        while len(resultset) > 0:
            eprint("Starting ProcessPoolExecutor with {} jobs".format(len(resultset)))
            with ProcessPoolExecutor(min(multiprocessing.cpu_count(), api.jobs)) as p:
                futures = {
                    p.submit(func, hash, local, tlim, mlim): (hash, local)
                    for (hash, local) in resultset[:max(multiprocessing.cpu_count(), api.jobs)]
                }
                try:
                    for f in as_completed(futures, timeout=tlim):
                        e = f.exception()
                        if e is not None:
                            if type(e) == BrokenProcessPool:
                                try:
                                    eprint("{}: {} in {}".format(e.__class__.__name__, e, futures[f]))
                                    resultset.remove(futures[f])
                                    break
                                except ValueError as err:
                                    eprint("Value error: {} {}".format(err, f))
                            elif type(e) == GBDException:
                                eprint("{}: {}".format(e.__class__.__name__, e))
                                return
                            else:
                                eprint("{}: {}".format(e.__class__.__name__, e))
                        else:
                            resultset.remove(futures[f])
                            api.set_attributes_locked(f.result())
                except TimeoutError as e:
                    eprint("{}: {}".format(e.__class__.__name__, e))
                except Exception as e:
                    eprint("{}: {}".format(e.__class__.__name__, e))


def init_transform_cnf_to_kis(api: GBD, query, hashes, tlim, mlim):
    resultset = api.query_search(query, hashes, ["local"], collapse="MIN")
    run(api, resultset, cnf_to_kis, tlim, mlim)

def cnf_to_kis(hashvalue, filename, tlim, mlim):
    output = filename
    for suffix in config.suffix_list('cnf'):
        output = output.removesuffix(suffix)
    output = output + ".kis"
    eprint('Transforming {} to k-ISP {}'.format(filename, output))
    transform_cnf_to_kis(filename, output)
    kishash = gbd_hash(output)
    return [ ('isp_local', output, kishash), ('translator_cnf_isp', kishash, hashvalue) ]


# Initialize base feature tables for given instances
def init_base_features(api: GBD, query, hashes, tlim, mlim):
    resultset = api.query_search(query, hashes, ["local"], collapse="MIN")
    run(api, resultset, base_features, tlim, mlim)

def base_features(hashvalue, filename, tlim, mlim):
    eprint('Extracting base features from {}'.format(filename))
    rec = extract_base_features(filename, tlim, mlim)
    eprint('Done with base features from {}'.format(filename))
    return [ (key, int(value) if isinstance(value, float) and value.is_integer() else value, hashvalue) for key, value in rec.items() ]


# Initialize gate feature tables for given instances
def init_gate_features(api: GBD, query, hashes, tlim, mlim):
    resultset = api.query_search(query, hashes, ["local"], collapse="MIN")
    run(api, resultset, gate_features, tlim, mlim)

def gate_features(hashvalue, filename, tlim, mlim):
    eprint('Extracting gate features from {}'.format(filename))
    rec = extract_gate_features(filename, tlim, mlim)
    eprint('Done with gate features from {}'.format(filename))
    return [ (key, int(value) if isinstance(value, float) and value.is_integer() else value, hashvalue) for key, value in rec.items() ]


# Initialize Graph Features known from Network Analysis
def init_networkit_features(api: GBD, query, hashes, tlim, mlim):
    nk.setNumberOfThreads(min(multiprocessing.cpu_count(), api.jobs))
    resultset = api.query_search(query, hashes, ["local"], collapse="MIN")
    for (hash, local) in resultset: 
        result = networkit_features(hash, local, tlim, mlim)
        eprint(result['hashvalue'])
        for att in result['attributes']:
            eprint(att[1] + "=" + att["2"])

def networkit_features(hashvalue, filename, tlim, mlim):
    rec = dict()
    return [ (key, int(value) if isinstance(value, float) and value.is_integer() else value, hashvalue) for key, value in rec.items() ]


# Initialize degree_sequence_hash for given instances
def init_degree_sequence_hash(api: GBD, hashes, tlim, mlim):
    if not api.feature_exists("degree_sequence_hash"):
        api.create_feature("degree_sequence_hash", "empty")
    resultset = api.query_search(None, hashes, ["local"], collapse="MIN")
    run(api, resultset, compute_degree_sequence_hash)

def compute_degree_sequence_hash(hashvalue, filename, tlim, mlim):
    eprint('Computing degree-sequence hash for {}'.format(filename))
    hash_md5 = hashlib.md5()
    degrees = dict()
    f = open_cnf_file(filename, 'rt')
    for line in f:
        line = line.strip()
        if line and line[0] not in ['p', 'c']:
            for lit in line.split()[:-1]:
                num = int(lit)
                tup = degrees.get(abs(num), (0,0))
                degrees[abs(num)] = (tup[0], tup[1]+1) if num < 0 else (tup[0]+1, tup[1])

    degree_list = list(degrees.values())
    degree_list.sort(key=lambda t: (t[0]+t[1], abs(t[0]-t[1])))
    
    for t in degree_list:
        hash_md5.update(str(t[0]+t[1]).encode('utf-8'))
        hash_md5.update(b' ')
        hash_md5.update(str(abs(t[0]-t[1])).encode('utf-8'))
        hash_md5.update(b' ')

    f.close()

    return [ ('degree_sequence_hash', hash_md5.hexdigest(), hashvalue) ]