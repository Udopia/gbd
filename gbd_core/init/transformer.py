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

import pebble
from concurrent.futures import as_completed

import pandas as pd

from gbd_core.api import GBD, GBDException
from gbd_core import util


class InstanceTransformer:

    def __init__(self, api: GBD, features, transformer, target_context, target_db=None):
        self.api = api
        self.api.database.set_auto_commit(False)
        self.features = features
        self.transformer = transformer
        self.target_context = target_context
        self.target_db = target_db


    def create_features(self):
        for (name, default) in self.features:
            self.api.database.create_feature(name, default, self.target_db, True)
        self.api.database.commit()


    def save_features(self, result: list):
        for attr in result:
            name, hashv, value = attr[0], attr[1], attr[2]
            self.api.database.set_values(name, value, [hashv], self.target_db)
        self.api.database.commit()


    def transform(self, instances: pd.DataFrame):
        if self.api.jobs == 1:
            self.transform_sequential(instances)
        else:
            self.transform_parallel(instances)


    def transform_sequential(self, instances: pd.DataFrame):
        for idx, row in instances.iterrows():
            result = self.transformer(row['hash'], row['local'], self.api.get_limits())
            self.save_features(result)


    def transform_parallel(self, instances: pd.DataFrame):
        with pebble.ProcessPool(max_workers=self.api.jobs, max_tasks=1) as p:
            futures = [ p.schedule(self.extractor, (row['hash'], row['local'], self.api.getlimits())) for idx, row in instances.iterrows() ]
            for f in as_completed(futures):  #, timeout=api.tlim if api.tlim > 0 else None):
                try:
                    result = f.result()
                    self.save_features(result)
                except pebble.ProcessExpired as e:
                    f.cancel()
                    util.eprint("{}: {}".format(e.__class__.__name__, e))
                except GBDException as e:  # might receive special handling in the future
                    util.eprint("{}: {}".format(e.__class__.__name__, e))
                except Exception as e:
                    util.eprint("{}: {}".format(e.__class__.__name__, e))

