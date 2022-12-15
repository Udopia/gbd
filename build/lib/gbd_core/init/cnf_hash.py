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

from gbd_core import contexts
from gbd_core.api import GBD
from gbd_core.util import eprint, confirm, slice_iterator, open_cnf_file

from gbd_core.init.runner import run

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


# Initialize table 'local' with instances found under given path
def init_local(api: GBD, root):
    clocal = contexts.prepend_context("local", api.context)
    api.database.create_feature(clocal, permissive=True)
    df = api.query(group_by=clocal)
    paths = set(row[clocal] for idx, row in df.iterrows())
    missing_files = [path for path in paths if not os.path.isfile(path)]
    if len(missing_files) and confirm("{} files not found. Remove stale entries from local table?".format(len(missing_files))):
        for paths_chunk in slice_iterator(missing_files, 1000):
            api.database.delete_values(clocal, paths_chunk)
    dfs = []
    for suffix in contexts.suffix_list(api.context):
        iter = glob.iglob(root + "/**/*" + suffix, recursive=True)
        df = pd.DataFrame([('None', path) for path in iter if not path in paths], columns=['hash', clocal])
        dfs.append(df)
    run(api, pd.concat(dfs), compute_hash, {'context': api.context, **api.get_limits()})


def compute_hash(hash, path, args):
    eprint('Hashing {}'.format(path))
    hashvalue = gbd_hash(path)
    clocal = contexts.prepend_context("local", args["context"])
    return [ (clocal, hashvalue, path) ]

# Initialize degree_sequence_hash for given instances
def init_iso_hash(api: GBD, query, hashes):
    if not api.feature_exists("isohash"):
        api.create_feature("isohash", "empty")
    df = api.query(query, hashes, ["local"], collapse="MIN")
    run(api, df, compute_iso_hash, api.get_limits())

def compute_iso_hash(hashvalue, filename, args):
    eprint('Computing iso hash for {}'.format(filename))
    isoh = isohash(filename)
    return [ ('isohash', hashvalue, isoh) ]