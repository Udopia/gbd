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


import multiprocessing

from gbd_core.api import GBD, GBDException
from gbd_core.util import eprint

from gbd_core.init.cnf_hash import gbd_hash
from gbd_core.init.runner import run

# Initialize Graph Features known from Network Analysis
def init_networkit_features(api: GBD, query, hashes):
    try:
        import networkit as nk
    except ImportError as e:
        raise GBDException("Module 'networkit' not found. Setup https://networkit.github.io/")
    nk.setNumberOfThreads(min(multiprocessing.cpu_count(), api.jobs))
    df = api.query(query, hashes, ["local"], collapse="MIN")
    for idx, row in df.iterrows(): 
        result = networkit_features(row['hash'], row['local'], {})
        eprint(result['hashvalue'])
        for att in result['attributes']:
            eprint(att[1] + "=" + att["2"])

def networkit_features(hashvalue, filename, args):
    rec = dict()
    # TODO: Calculate Networkit Features
    return [ (key, hashvalue, int(value) if isinstance(value, float) and value.is_integer() else value) for key, value in rec.items() ]
