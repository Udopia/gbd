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
from gbd_core.util import eprint
from gbd_core.init.extractor import FeatureExtractor

from gbdc import extract_gate_features


class CNFGate(FeatureExtractor):

    def __init__(self, api: GBD, target_db):
        if not api.context in [ 'cnf' ]:
            raise GBDException("Context '{}' not supported by CNFBase".format(api.context))
        self.features = [ ("gate_features_runtime", "empty"), ("n_vars", "empty"), ("n_gates", "empty"), ("n_roots", "empty"),
            ("n_none", "empty"), ("n_generic", "empty"), ("n_mono", "empty"), ("n_and", "empty"), ("n_or", "empty"), ("n_triv", "empty"), ("n_equiv", "empty"), ("n_full", "empty"),
            ("levels_mean", "empty"), ("levels_variance", "empty"), ("levels_min", "empty"), ("levels_max", "empty"), ("levels_entropy", "empty"),
            ("levels_none_mean", "empty"), ("levels_none_variance", "empty"), ("levels_none_min", "empty"), ("levels_none_max", "empty"), ("levels_none_entropy", "empty"),
            ("levels_generic_mean", "empty"), ("levels_generic_variance", "empty"), ("levels_generic_min", "empty"), ("levels_generic_max", "empty"), ("levels_generic_entropy", "empty"),
            ("levels_mono_mean", "empty"), ("levels_mono_variance", "empty"), ("levels_mono_min", "empty"), ("levels_mono_max", "empty"), ("levels_mono_entropy", "empty"),
            ("levels_and_mean", "empty"), ("levels_and_variance", "empty"), ("levels_and_min", "empty"), ("levels_and_max", "empty"), ("levels_and_entropy", "empty"),
            ("levels_or_mean", "empty"), ("levels_or_variance", "empty"), ("levels_or_min", "empty"), ("levels_or_max", "empty"), ("levels_or_entropy", "empty"),
            ("levels_triv_mean", "empty"), ("levels_triv_variance", "empty"), ("levels_triv_min", "empty"), ("levels_triv_max", "empty"), ("levels_triv_entropy", "empty"),
            ("levels_equiv_mean", "empty"), ("levels_equiv_variance", "empty"), ("levels_equiv_min", "empty"), ("levels_equiv_max", "empty"), ("levels_equiv_entropy", "empty"),
            ("levels_full_mean", "empty"), ("levels_full_variance", "empty"), ("levels_full_min", "empty"), ("levels_full_max", "empty"), ("levels_full_entropy", "empty") ]
        super().__init__(api, self.features, self.compute_gate_features, target_db)

    def compute_gate_features(self, hash, path, limits):
        eprint('Extracting gate features from {} {}'.format(hash, path))
        rec = extract_gate_features(path, limits['tlim'], limits['mlim'])
        return [ (key, hash, int(value) if isinstance(value, float) and value.is_integer() else value) for key, value in rec.items() ]

# Initialize gate feature tables for given instances
def init_gate_features(api: GBD, query, hashes, target_db):
    extractor = CNFGate(api, target_db)
    extractor.create_features()
    df = api.query(query, hashes, ["local"], collapse="MIN")
    extractor.extract(df)