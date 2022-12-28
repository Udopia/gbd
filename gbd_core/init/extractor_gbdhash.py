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
import io
import hashlib
import glob
import pandas as pd
import itertools

from gbd_core import contexts
from gbd_core.api import GBD, GBDException
from gbd_core.util import eprint, confirm, open_cnf_file
from gbd_core.init.extractor import FeatureExtractor

from gbdc import isohash

try:
    from gbdc import gbdhash as gbd_hash
except ImportError:
    try:
        from gbdhashc import gbdhash as gbd_hash
    except ImportError:
        def gbd_hash(filename):
            file = open_cnf_file(filename, 'rb')
            buff = io.BufferedReader(file, io.DEFAULT_BUFFER_SIZE * 16)

            space = False
            skip = False
            start = True
            cldelim = True
            hash_md5 = hashlib.md5()

            for byte in iter(lambda: buff.read(1), b''):
                if not skip and (byte >= b'0' and byte <= b'9' or byte == b'-'):
                    cldelim = byte == b'0' and (space or start)
                    start = False
                    if space:
                        space = False
                        hash_md5.update(b' ')
                    hash_md5.update(byte)
                elif byte <= b' ':
                    space = not start # remember non-leading space characters 
                    skip = skip and byte != b'\n' and byte != b'\r' # comment line ended
                else: #byte == b'c' or byte == b'p':
                    skip = True  # do not hash comment and header line

            if not cldelim:
                hash_md5.update(b' 0')

            file.close()

            return hash_md5.hexdigest()


class GBDHash(FeatureExtractor):

    def __init__(self, api: GBD, target_db):
        if not api.context in [ 'cnf', 'sancnf', 'cnf2' ]:
            raise GBDException("Context '{}' not supported by GBDHash".format(api.context))
        if api.database.dcontext(target_db) != api.context:
            raise GBDException("Target database '{}' has context '{}' but expected '{}'".format(target_db, api.database.dcontext(dbname=target_db), api.context))
        self.features = [ ("local", None) ]
        super().__init__(api, self.features, self.compute_hash, target_db)

    def compute_hash(self, hash, path, limits):
        eprint('Hashing {}'.format(path))
        hash = gbd_hash(path)
        return [ (self.features[0][0], hash, path) ]


# Initialize table 'local' with instances found under given path
def init_local(api: GBD, root, target_db):
    extractor = GBDHash(api, target_db)
    extractor.create_features()

    df = api.query(group_by="local")
    dfilter = df["local"].apply(lambda x: not x or not os.path.isfile(x))

    missing = df[dfilter]
    if len(missing) and confirm("{} files not found. Remove stale entries from local table?".format(len(missing))):
        api.remove_attributes("local", values=missing["local"].tolist())

    paths = [ path for suffix in contexts.suffix_list(api.context) for path in glob.iglob(root + "/**/*" + suffix, recursive=True) ]
    df2 = pd.DataFrame([(None, path) for path in paths if not path in df["local"].to_list()], columns=["hash", "local"])
    
    extractor.extract(df2)