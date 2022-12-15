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

from gbd_core.api import GBD, GBDException
from gbd_core.util import eprint

def safe_run_results(api: GBD, result, check=False):
    for attr in result:
        name, hashv, value = attr[0], attr[1], attr[2]
        eprint("Saving {}={} for {}".format(name, value, hashv))
        if check and not name in api.database.get_features():
            if name.endswith("_local"):
                api.database.create_feature(name)
            else:
                api.database.create_feature(name, "empty")
        api.database.set_values(name, value, [hashv])

# Parallel Runner
def run(api: GBD, df, func, args: dict):
    first = True
    if api.jobs == 1:
        for idx, row in df.iterrows():
            result = func(row['hash'], row['local'], args)
            safe_run_results(api, result, check=first)
            first = False
    else:
        njobs=min(multiprocessing.cpu_count(), api.jobs)
        with pebble.ProcessPool(max_workers=njobs, max_tasks=1) as p:
            futures = [ p.schedule(func, (row['hash'], row['local'], args)) for idx, row in df.iterrows() ]
            for f in as_completed(futures):  #, timeout=api.tlim if api.tlim > 0 else None):
                try:
                    result = f.result()
                    safe_run_results(api, result, check=first)
                    first = False
                except pebble.ProcessExpired as e:
                    f.cancel()
                    eprint("{}: {}".format(e.__class__.__name__, e))
                except GBDException as e:  # might receive special handling in the future
                    eprint("{}: {}".format(e.__class__.__name__, e))
                except Exception as e:
                    eprint("{}: {}".format(e.__class__.__name__, e))
