
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
import time
import pebble
from concurrent.futures import as_completed
import pandas as pd

from gbd_core.api import GBD, GBDException
from gbd_core import util
import os

def prep_data(rec, hash):
    print('Extracting features from {}'.format(hash))
    return [(key, hash, int(value) if isinstance(value, float) and value.is_integer() else value) for key, value in
            rec.items()]


class InitializerException(Exception):
    pass

class Initializer:

    def __init__(self, api: GBD, rlimits: dict, target_db: str, features: list, initfunc, usepool=False):
        self.api = api
        self.api.database.set_auto_commit(False)
        self.target_db = target_db
        self.features = features
        self.initfunc = initfunc
        self.rlimits = rlimits
        self.usepool = usepool


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
        elif self.usepool:
            self.init_parallel_tp(instances)
        else:
            self.init_parallel_pp(instances)

    def init_sequential(self, instances: pd.DataFrame):
        for idx, row in instances.iterrows():
            result = self.initfunc(row['hash'], row['local'], self.rlimits)
            self.save_features(result)


    def init_parallel_tp(self, instances: pd.DataFrame):
        args = {row['local']: row['hash'] for idx, row in instances.iterrows() if row['local'] != 'None'}
        paths = [(key,) for key in args.keys()]
        q = self.initfunc(self.rlimits['mlim']*int(1e6), self.rlimits['jobs'], paths)
        while not q.done():
            if not q.empty():
                result = q.pop()
                rec = result[0]
                success = result[1]
                path = result[2]
                hash = args[path]
                # if computation successful
                if not success:
                    print('Failed to extract features from {}'.format(path))
                data = prep_data(rec, hash)
                self.save_features(data)
            else:
                time.sleep(0.5)

    def init_parallel_pp(self, instances: pd.DataFrame):
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

