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


from gbd_core.api import GBD, GBDException
from gbd_core import util
from gbd_core.init.extractor import FeatureExtractor

from gbdc import isohash

class ISOHash(FeatureExtractor):

    def __init__(self, api: GBD, target_db):
        if not api.context in [ 'cnf', 'sancnf', 'cnf2' ]:
            raise GBDException("Context '{}' not supported by GBDHash".format(api.context))
        self.features = [ ('isohash', 'empty') ]
        super().__init__(api, self.features, self.compute_hash, target_db)

    def compute_hash(self, hash, path, args):
        util.eprint('Computing ISOHash for {}'.format(path))
        isohash = isohash(path)
        return [ ('isohash', hash, isohash) ]


# Initialize degree_sequence_hash for given instances
def init_iso_hash(api: GBD, query, hashes, target_db):
    extractor = ISOHash(api, target_db)
    extractor.create_features()
    df = api.query(query, hashes, ["local"], collapse="MIN")
    extractor.extract_features(df)