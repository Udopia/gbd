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

import multiprocessing
import pebble
from concurrent.futures import as_completed
import pandas as pd

from gbd_core.api import GBD, GBDException
from gbd_core import util


class Initializer:

    def __init__(self, source_contexts: list, target_contexts: list, api: GBD, target_db: str, features: list, initfunc):
        self.api = api
        self.api.database.set_auto_commit(False)
        self.target_db = target_db
        self.features = features
        self.initfunc = initfunc
        if not api.context in source_contexts:
            raise GBDException("Context '{}' not supported by '{}'".format(api.context, self.__class__.__name__))
        if not api.database.dcontext(target_db) in target_contexts:
            raise GBDException("Target database '{}' has incompatible context '{}'".format(target_db, api.database.dcontext(target_db)))


    def create_features(self):
        for (name, default) in self.features:
            self.api.database.create_feature(name, default, self.target_db, True)
        self.api.database.commit()


    def save_features(self, result: list):
        for attr in result:
            name, hashv, value = attr[0], attr[1], attr[2]
            self.api.database.set_values(name, value, [hashv], self.target_db)
        self.api.database.commit()


    def run(self, instances: pd.DataFrame):
        if self.api.jobs == 1:
            self.init_sequential(instances)
        else:
            self.init_parallel(instances)


    def init_sequential(self, instances: pd.DataFrame):
        for idx, row in instances.iterrows():
            result = self.initfunc(row['hash'], row['local'], self.api.get_limits())
            self.save_features(result)


    def init_parallel(self, instances: pd.DataFrame):
        with pebble.ProcessPool(max_workers=self.api.jobs, max_tasks=1, context=multiprocessing.get_context('forkserver')) as p:
            futures = [ p.schedule(self.initfunc, (row['hash'], row['local'], self.api.get_limits())) for idx, row in instances.iterrows() ]
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
                    import traceback
                    traceback.print_exc()
                    util.eprint("{}: {}".format(e.__class__.__name__, e))

