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
import shutil

from gbd_core import util
from gbd_core.api import GBD

from gbd_core.init.cnf_hash import gbd_hash
from gbd_core.init.runner import run

from gbdc import sanitize


# Sanitize given instances
def init_sani(api: GBD, query, hashes):
    api.database.create_feature("sancnf_local", permissive=True)
    api.database.create_feature("cnf_to_sancnf", permissive=True)
    api.database.create_feature("sancnf_to_cnf", permissive=True)

    resultset = []

    df = api.query(query, hashes, ["local"])
    for idx, row in df.iterrows():
        hashv = row['hash']
        local = row['local']
        if local:
            missing = []
            for path in local.split(","):
                sanname = os.path.splitext(path)[0]
                if not os.path.isfile(sanname):
                    missing.append(path)
            if len(missing):
                resultset.append([hashv, ",".join(missing)])
        else:
            util.eprint("Entry not found: " + hashv)
            return []

    run(api, resultset, compute_sani, api.get_limits())

def compute_sani(hashvalue, paths, args):
    result = []

    sanname = None
    sanhash = None
    for path in paths.split(","):
        util.eprint('Sanitizing {}'.format(path))

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
