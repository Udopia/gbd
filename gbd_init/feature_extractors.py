# MIT License

# Copyright (c) 2025 Ashlin Iser, Karlsruhe Institute of Technology (KIT)

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

import os
import glob
import polars as pl
from functools import partial

from gbd_core.contexts import suffixes, identify
from gbd_core.api import GBD
from gbd_core.util import eprint, confirm
from gbd_init.initializer import Initializer, InitializerException
from gbd_init import external


## GBDHash (local paths): computed in-process, does not require an external tool.
def compute_hash(hash, path, limits):
    eprint("Hashing {}".format(path))
    hash = identify(path)
    return [("local", hash, path), ("filename", hash, os.path.basename(path))]


## Generic external-tool extractor
def _compute_extractor(hash, path, limits, tool):
    eprint("Running {} on {}".format(tool, path))
    try:
        values, status = external.run_extractor(tool, path, limits)
    except external.ExternalToolException as e:
        eprint(str(e))
        return []
    if status != "success":
        eprint("{}: {} {}".format(status, tool, path))
        return []
    return [(key, hash, external.convert(value)) for key, value in values.items()]


def build_extractors(gbdconfig):
    """Build the extractor registry ``name -> spec`` from the configuration."""
    registry = {}
    for name, spec in gbdconfig.extractors.items():
        registry[name] = {
            "description": spec.get("description", name),
            "contexts": spec.get("contexts", []),
            "tool": spec["tool"],
        }
    return registry


def init_features_generic(key: str, api: GBD, rlimits, df: pl.DataFrame, target_db, registry):
    einfo = registry[key]
    context = api.database.dcontext(target_db)
    if context not in einfo["contexts"]:
        raise InitializerException("Target database context must be in {}".format(einfo["contexts"]))
    features = external.feature_names(einfo["tool"])
    compute = partial(_compute_extractor, tool=einfo["tool"])
    extractor = Initializer(api, rlimits, target_db, features, compute)
    extractor.create_features()
    extractor.run(df)


def init_local(api: GBD, rlimits, root, target_db):
    context = api.database.dcontext(target_db)

    features = [("local", None), ("filename", None)]
    extractor = Initializer(api, rlimits, target_db, features, compute_hash)
    extractor.create_features()

    # Cleanup stale entries
    df: pl.DataFrame = api.query(group_by=context + ":local", collapse=None)
     
    def path_exists(p):
        return p is not None and os.path.exists(p)
    
    missing = df.with_columns(
        exists=pl.col("local").map_elements(
            path_exists,
            return_dtype=pl.Boolean
        )
    ).filter(~pl.col("exists")).select("local")
    
    if len(missing) and api.verbose:
        for path in missing["local"].to_list():
            eprint(path)
    if len(missing) and confirm("{} files not found. Remove stale entries from local table?".format(len(missing))):
        api.reset_values("local", values=missing["local"].to_list())

    # Create df with paths not yet in local table
    paths = [path for suffix in suffixes(context) for path in glob.iglob(root + "/**/*" + suffix, recursive=True)]
    df2 = pl.DataFrame([(None, path) for path in paths if path not in df["local"].to_list()], schema=["hash", "local"], orient="row")

    extractor.run(df2)
