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
from functools import partial, reduce

import polars as pl

from gbd_core import contexts, util
from gbd_core.api import GBD
from gbd_core.util import convert
from gbd_init import external
from gbd_init.initializer import Initializer, InitializerException

_COMPRESSION_SUFFIX = {"xz": ".xz", "gz": ".gz", "bz2": ".bz2"}


def _strip_context_suffix(path, source_context):
    return reduce(lambda p, suffix: p[: -len(suffix)] if p.endswith(suffix) else p, contexts.suffixes(source_context), path)


def _output_path(path, source_context, output_suffix):
    return _strip_context_suffix(path, source_context) + output_suffix


def _final_path(output_path, compress):
    return output_path + _COMPRESSION_SUFFIX.get(compress, "")


def _remove(path):
    if path and os.path.exists(path):
        try:
            os.remove(path)
        except OSError:
            pass


## Generic external-tool transformer
def _compute_transformer(hash, path, limits, tool, source_context, output_suffix, compress):
    output = _output_path(path, source_context, output_suffix)
    final = _final_path(output, compress)
    util.eprint(f"Transforming {path} -> {final}")
    try:
        values, status = external.run_external_tool(tool, path, limits, output, compress)
    except external.ExternalToolException as e:
        util.eprint(str(e))
        _remove(final)
        return []
    if status != "success":
        util.eprint(f"{status}: {tool} {path}")
        _remove(final)
        return []
    newhash = values.pop("hash", None)
    if not newhash:
        util.eprint(f"Transformer {tool} produced no hash for {path}")
        _remove(final)
        return []
    return [(key, newhash, convert(value)) for key, value in values.items()]


def build_transformers(gbdconfig):
    """Build the transformer registry ``name -> spec`` from the configuration."""
    registry = {}
    for name, spec in gbdconfig.transformers.items():
        registry[name] = {
            "description": spec.get("description", name),
            "tool": spec["tool"],
            "source": spec.get("source", []),
            "target": spec.get("target", []),
            "compress": spec.get("compress", "none"),
            "output_suffix": spec.get("output_suffix"),
        }
    return registry


def transform_instances_generic(key: str, api: GBD, rlimits, query, hashes, target_db, source, registry, collapse=None):
    einfo = registry[key]
    target_context = api.database.dcontext(target_db)
    if target_context not in einfo["target"]:
        raise InitializerException(f"Target database context must be in {einfo['target']}")
    if source not in einfo["source"]:
        raise InitializerException(f"Source context must be in {einfo['source']}")

    # Output filename: source path with its context suffix replaced by the target suffix
    # (a per-transformer `output_suffix` overrides the target context's suffix).
    output_suffix = einfo["output_suffix"] or contexts.config[target_context]["suffix"]
    compress = einfo["compress"]

    features = external.feature_names(einfo["tool"])
    compute = partial(
        _compute_transformer,
        tool=einfo["tool"],
        source_context=source,
        output_suffix=output_suffix,
        compress=compress,
    )
    transformer = Initializer(api, rlimits, target_db, features, compute)
    transformer.create_features()

    df: pl.DataFrame = api.query(query, hashes, [source + ":local"], collapse=collapse)

    def not_yet_transformed(p):
        return p is not None and not os.path.exists(_final_path(_output_path(p, source, output_suffix), compress))

    missing = df.with_columns(
        todo=pl.col("local").map_elements(not_yet_transformed, return_dtype=pl.Boolean)
    ).filter(pl.col("todo"))

    transformer.run(missing)
