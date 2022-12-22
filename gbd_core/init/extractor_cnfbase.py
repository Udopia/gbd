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

from gbdc import extract_base_features


class CNFBase(FeatureExtractor):

    def __init__(self, api: GBD, target_db):
        if not api.context in [ 'cnf' ]:
            raise GBDException("Context '{}' not supported by CNFBase".format(api.context))
        self.features = [ ("base_features_runtime", "empty"), ("clauses", "empty"), ("variables", "empty"), ("clause_size_1", "empty"), ("clause_size_2", "empty"), ("clause_size_3", "empty"), 
            ("clause_size_4", "empty"), ("clause_size_5", "empty"), ("clause_size_6", "empty"), ("clause_size_7", "empty"), ("clause_size_8", "empty"), ("clause_size_9", "empty"), 
            ("horn_clauses", "empty"), ("inv_horn_clauses", "empty"), ("positive_clauses", "empty"), ("negative_clauses", "empty"),
            ("horn_vars_mean", "empty"), ("horn_vars_variance", "empty"), ("horn_vars_min", "empty"), ("horn_vars_max", "empty"), ("horn_vars_entropy", "empty"),
            ("inv_horn_vars_mean", "empty"), ("inv_horn_vars_variance", "empty"), ("inv_horn_vars_min", "empty"), ("inv_horn_vars_max", "empty"), ("inv_horn_vars_entropy", "empty"),
            ("vg_degrees_mean", "empty"), ("vg_degrees_variance", "empty"), ("vg_degrees_min", "empty"), ("vg_degrees_max", "empty"), ("vg_degrees_entropy", "empty"),
            ("balance_clause_mean", "empty"), ("balance_clause_variance", "empty"), ("balance_clause_min", "empty"), ("balance_clause_max", "empty"), ("balance_clause_entropy", "empty"),
            ("balance_vars_mean", "empty"), ("balance_vars_variance", "empty"), ("balance_vars_min", "empty"), ("balance_vars_max", "empty"), ("balance_vars_entropy", "empty"),
            ("vcg_vdegrees_mean", "empty"), ("vcg_vdegrees_variance", "empty"), ("vcg_vdegrees_min", "empty"), ("vcg_vdegrees_max", "empty"), ("vcg_vdegrees_entropy", "empty"),
            ("vcg_cdegrees_mean", "empty"), ("vcg_cdegrees_variance", "empty"), ("vcg_cdegrees_min", "empty"), ("vcg_cdegrees_max", "empty"), ("vcg_cdegrees_entropy", "empty"),
            ("cg_degrees_mean", "empty"), ("cg_degrees_variance", "empty"), ("cg_degrees_min", "empty"), ("cg_degrees_max", "empty"), ("cg_degrees_entropy", "empty") ]
        super().__init__(api, self.features, self.compute_base_features, target_db)

    def compute_base_features(self, hash, path, limits):
        eprint('Extracting base features from {} {}'.format(hash, path))
        rec = extract_base_features(path, limits['tlim'], limits['mlim'])
        return [ (key, hash, int(value) if isinstance(value, float) and value.is_integer() else value) for key, value in rec.items() ]

# Initialize base feature tables for given instances
def init_base_features(api: GBD, query, hashes, target_db):
    extractor = CNFBase(api, target_db)
    extractor.create_features()
    df = api.query(query, hashes, ["local"], collapse="MIN")
    extractor.extract(df)
