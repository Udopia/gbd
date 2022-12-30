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
from functools import reduce

from gbd_core import contexts
from gbd_core.api import GBD, GBDException
from gbd_core import util

from gbd_init.gbdhash import gbd_hash
from gbd_init.initializer import Initializer

from gbdc import cnf2kis, sanitize


# Transform SAT Problem to k-Independent Set Problem
def kis_filename(cls, path):
    kispath = reduce(lambda path, suffix: path[:-len(suffix)] if path.endswith(suffix) else path, contexts.suffix_list('cnf'), path)
    return kispath + '.kis'

def cnf2kis(self, hash, path, limits):
    util.eprint('Transforming {} to k-ISP {}'.format(path, kispath))
    
    kispath = kis_filename(path)
    try:
        result = cnf2kis(path, kispath, limits['max_edges'], limits['max_nodes'], limits['tlim'], limits['mlim'], limits['flim'])
        if "local" in result:
            kishash = result['hash']
            return [ ('local', kishash, result['local']), ('cnf_hash', kishash, hash),
                        ('nodes', kishash, result['nodes']), ('edges', kishash, result['edges']), ('k', kishash, result['k']) ]
        else:
            raise GBDException("CNF2KIS failed for {}".format(path))
    except Exception as e:
        util.eprint(str(e))
        os.remove(kispath)

    return [ ]

def init_transform_cnf_to_kis(api: GBD, query, hashes, target_db=None):
    source_contexts = [ 'cnf', 'sancnf' ]
    target_contexts = [ 'kis' ]
    features = [ ('local', None), ('cnf_hash', None), ('nodes', 'empty'), ('edges', 'empty'), ('k', 'empty') ]
    transformer = Initializer(source_contexts, target_contexts, api, target_db, features, cnf2kis)
    transformer.create_features()

    df = api.query(query, hashes, ["local"], collapse=None)
    dfilter = df['local'].apply(lambda x: x and not os.path.isfile(kis_filename(x)))

    transformer.run(df[dfilter])


# Sanitize CNF
def sanitized_filename(cls, path):
    sanpath = reduce(lambda path, suffix: path[:-len(suffix)] if path.endswith(suffix) else path, contexts.suffix_list('cnf'), path)
    return sanpath + '.sanitized.cnf'

def sanitize_cnf(self, hash, path, limits):
    util.eprint('Sanitizing {}'.format(path))

    sanname = sanitized_filename(path)
    try:
        with open(sanname, 'w') as f, util.stdout_redirected(f):
            if sanitize(path): 
                sanhash = gbd_hash(sanname)
                return [ ('local', sanhash, sanname), ('cnf_hash', sanhash, hash) ]
            else:
                raise GBDException("Sanitization failed for {}".format(path))
    except Exception as e:
        util.eprint(str(e))
        os.remove(sanname)

    return [ ]

def init_sani(api: GBD, query, hashes, target_db=None):
    source_contexts = [ 'cnf', 'sancnf' ]
    target_contexts = [ 'sancnf' ]
    features = [ ('local', None) , ('cnf_hash', None) ]
    transformer = Initializer(source_contexts, target_contexts, api, target_db, features, sanitize_cnf)
    transformer.create_features()

    df = api.query(query, hashes, ["local"], collapse=None)
    dfilter = df['local'].apply(lambda x: x and not os.path.isfile(sanitized_filename(x)))

    transformer.run(df[dfilter])

