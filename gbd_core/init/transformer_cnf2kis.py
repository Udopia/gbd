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
from gbd_core import util

from gbd_core.init.extractor_gbdhash import gbd_hash
from gbd_core.init.transformer import InstanceTransformer

from gbdc import cnf2kis

# transform cnf to k-independent set problem
class CNF2KIS(InstanceTransformer):

    def __init__(self, api: GBD, target_db=None):
        if not api.context in [ 'cnf' ]:
            raise GBDException("Context '{}' not supported by CNFBase".format(api.context))
        self.features = [ ('kis_local', None), ('cnf_to_kis', None), ('kis_to_cnf', None), ('kis_nodes', 'empty'), ('kis_edges', 'empty'), ('kis_k', 'empty') ]
        super().__init__(api, self.features, self.cnf2kis, target_context='kis', target_db=target_db)

    @classmethod
    def kis_filename(cls, path):
        kispath = reduce(lambda path, suffix: path[:-len(suffix)] if path.endswith(suffix) else path, contexts.suffix_list('cnf'), path)
        return kispath + '.kis'

    def cnf2kis(self, hash, path, limits):
        util.eprint('Transforming {} to k-ISP {}'.format(path, kispath))
        
        kispath = CNF2KIS.kis_filename(path)
        try:
            result = cnf2kis(path, kispath, limits['max_edges'], limits['max_nodes'], limits['tlim'], limits['mlim'], limits['flim'])
            if "local" in result:
                return [ ('kis_local', result['hash'], result['local']), ('cnf_to_kis', hash, result['hash']), ('kis_to_cnf', result['hash'], hash),
                        ('kis_nodes', result['hash'], result['nodes']), ('kis_edges', result['hash'], result['edges']), ('kis_k', result['hash'], result['k']) ]
            else:
                raise GBDException("CNF2KIS failed for {}".format(path))
        except Exception as e:
            util.eprint(str(e))
            os.remove(kispath)

        return [ ]


def init_transform_cnf_to_kis(api: GBD, query, hashes, target_db=None):
    transformer = CNF2KIS(api, target_db)
    transformer.create_features()

    df = api.query(query, hashes, ["local"], collapse=None)
    dfilter = df['local'].apply(lambda x: x and not os.path.isfile(CNF2KIS.kis_filename(x)))

    transformer.transform(df[dfilter])

