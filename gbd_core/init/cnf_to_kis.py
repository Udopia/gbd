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

from gbd_core import contexts
from gbd_core.api import GBD, GBDException
from gbd_core.util import eprint

from gbd_core.init.cnf_hash import gbd_hash
from gbd_core.init.runner import run

from gbdc import cnf2kis


def init_transform_cnf_to_kis(api: GBD, query, hashes, max_edges, max_nodes):
    api.database.create_feature('kis_local', permissive=True)
    api.database.create_feature('kis_nodes', "empty", permissive=True)
    api.database.create_feature('kis_edges', "empty", permissive=True)
    api.database.create_feature('kis_k', "empty", permissive=True)
    api.database.create_feature('cnf_to_kis', permissive=True)
    api.database.create_feature('kis_to_cnf', permissive=True)
    df = api.query(query, hashes, ["local"], collapse="MIN")
    run(api, df, transform_cnf_to_kis, { **api.get_limits(), 'max_edges': max_edges, 'max_nodes': max_nodes })

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

