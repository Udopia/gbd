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

from gbd_init.gbdhash import cnf_hash, opb_hash, wcnf_hash
from gbd_core import config as _config

### Registry of known identifier (hash) functions, keyed by their config name
_IDFUNCS = {
    "cnf_hash": cnf_hash,
    "opb_hash": opb_hash,
    "wcnf_hash": wcnf_hash,
}

### Default Context and Available Contexts (populated from configuration)
default = "cnf"
config = {}


def _build(gbdconfig):
    """(Re)build the module-level ``config`` and ``default`` from a :class:`GbdConfig`."""
    global config, default
    raw = gbdconfig.contexts
    built = {}
    for name, details in raw.items():
        if name == "default" or not isinstance(details, dict):
            continue
        built[name] = {
            "description": details.get("description", name),
            "suffix": details["suffix"],
            "idfunc": _IDFUNCS.get(details.get("idfunc", "cnf_hash"), cnf_hash),
        }
    config = built
    default = raw.get("default", "cnf")


def reload(config=None):
    """Rebuild the available contexts from a :class:`GbdConfig` (or configuration
    layers/paths, or the bundled defaults when None)."""
    if not isinstance(config, _config.GbdConfig):
        config = _config.GbdConfig(config)
    _build(config)


### Initial load from the environment (bundled defaults + optional GBD config)
_build(_config.default_config())


def description(context):
    return config[context]["description"]


def suffixes(context):
    return [config[context]["suffix"] + p for p in ["", ".gz", ".lzma", ".xz", ".bz2"]]


def idfunc(context):
    return config[context]["idfunc"]


def contexts():
    return config.keys()


def default_context():
    return default


def get_context_by_suffix(benchmark):
    for context in contexts():
        for suffix in suffixes(context):
            if benchmark.endswith(suffix):
                return context
    return None


def identify(path, ct=None):
    context = ct or get_context_by_suffix(path)
    if context is None:
        raise Exception("Unable to associate context: " + path)
    else:
        idf = idfunc(context)
        return idf(path)
