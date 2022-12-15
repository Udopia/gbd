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



from gbd_core.api import GBD
from gbd_core.util import eprint
from gbd_core.init.runner import run

from gbdc import extract_base_features


# Initialize base feature tables for given instances
def init_base_features(api: GBD, query, hashes):
    df = api.query(query, hashes, ["local"], collapse="MIN")
    run(api, df, base_features, api.get_limits())

def base_features(hashvalue, filename, args):
    eprint('Extracting base features from {} {}'.format(hashvalue, filename))
    rec = extract_base_features(filename, args['tlim'], args['mlim'])
    return [ (key, hashvalue, int(value) if isinstance(value, float) and value.is_integer() else value) for key, value in rec.items() ]