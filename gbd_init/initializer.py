
# MIT License

# Copyright (c) 2023 Markus Iser, Karlsruhe Institute of Technology (KIT)

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

import multiprocessing
import pebble
from concurrent.futures import as_completed
import pandas as pd

from gbd_core.api import GBD, GBDException
from gbd_core import util

class InitializerException(Exception):
    pass

class Initializer:

    def __init__(self, api: GBD, rlimits: dict, target_db: str, features: list, initfunc):
        self.api = api
        self.api.database.set_auto_commit(False)
        self.target_db = target_db
        self.features = features
        self.initfunc = initfunc
        self.rlimits = rlimits


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
        if self.rlimits['jobs'] == 1:
            self.init_sequential(instances)
        else:
            self.init_parallel(instances)


    def init_sequential(self, instances: pd.DataFrame):
        for idx, row in instances.iterrows():
            result = self.initfunc(row['hash'], row['local'], self.rlimits)
            self.save_features(result)


    def init_parallel(self, instances: pd.DataFrame):
        with pebble.ProcessPool(max_workers=self.rlimits['jobs'], max_tasks=1, context=multiprocessing.get_context('forkserver')) as p:
            futures = [ p.schedule(self.initfunc, (row['hash'], row['local'], self.rlimits)) for idx, row in instances.iterrows() ]
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

