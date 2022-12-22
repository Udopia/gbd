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
from gbd_core.api import GBD, GBDException

from gbd_core.init.extractor_gbdhash import gbd_hash
from gbd_core.init.transformer import InstanceTransformer

from gbdc import sanitize


class CNFSanitizer(InstanceTransformer):

    def __init__(self, api: GBD, target_db=None):
        if not api.context in [ 'cnf' ]:
            raise GBDException("Context '{}' not supported by CNFBase".format(api.context))
        self.features = [ ('sancnf_local', None) , ('cnf_to_sancnf', None), ('sancnf_to_cnf', None) ]
        super().__init__(api, self.features, self.sanitize_cnf, target_context='sancnf', target_db=target_db)

    @classmethod
    def sanitized_filename(cls, path):
        return path + '.sancnf'

    def sanitize_cnf(self, hash, path, limits):
        util.eprint('Sanitizing {}'.format(path))

        sanname = CNFSanitizer.sanitized_filename(path)
        try:
            with open(sanname, 'w') as f, util.stdout_redirected(f):
                if sanitize(path): 
                    sanhash = gbd_hash(sanname)
                    return [ ('sancnf_local', sanhash, sanname), ('sancnf_to_cnf', sanhash, hash), ('cnf_to_sancnf', hash, sanhash) ]
                else:
                    raise GBDException("Sanitization failed for {}".format(path))
        except Exception as e:
            util.eprint(str(e))
            os.remove(sanname)

        return [ ]


def init_sani(api: GBD, query, hashes, target_db=None):
    transformer = CNFSanitizer(api, target_db)
    transformer.create_features()

    df = api.query(query, hashes, ["local"], collapse=None)
    dfilter = df['local'].apply(lambda x: x and not os.path.isfile(CNFSanitizer.sanitized_filename(x)))

    transformer.transform(df[dfilter])

