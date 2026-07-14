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

"""GBD configuration system.

The central configuration is a TOML file registered via the ``GBD`` environment
variable (or passed to ``-d/--db`` as a path). It may declare databases, contexts,
extractors, and transformers. The bundled ``default_config.toml`` provides the
built-in defaults, onto which any user configuration is merged.

Configuration layers are merged low-to-high precedence:
``bundled defaults`` < ``GBD`` env config < ``-d/--db`` config. The ``GBD`` config
acts as a user-level default that a ``-d/--db`` config overrides. A ``-d/--db`` value
is auto-detected as either a legacy colon-separated database list or a path to a
central config file (see :func:`is_config_file`); a legacy database list (or the
``GBD_DB`` environment variable) provides the databases when no config layer does.

Configuration file format
--------------------------

A GBD configuration is a TOML document with up to four top-level tables. A file is
recognised as a config (rather than a database list) as soon as it contains at least
one of them. All tables are optional; anything omitted keeps its bundled default.

``[databases]``
    Extra databases loaded in addition to any given via ``-d/--db`` or ``GBD_DB``.

    - ``paths`` (list of str): filesystem paths to SQLite database files.

``[contexts]``
    Problem domains, keyed by context name. Each context maps a file suffix to the
    identifier (hash) function used for that domain.

    - ``default`` (str): the context assumed when none is given on the CLI.
    - ``<name>`` (table): a context definition with the keys

      - ``suffix`` (str): file-name suffix that identifies instances of this context
        (e.g. ``".cnf"``).
      - ``idfunc`` (str): name of the hash function used to compute instance
        identifiers (e.g. ``"cnf_hash"``, ``"opb_hash"``, ``"wcnf_hash"``).
      - ``description`` (str): human-readable description.

``[extractors]``
    Feature extractors: external command-line tools that compute features for an
    instance. Keyed by extractor name; each entry supports

    - ``tool`` (str): the command gbd runs. It may include a subcommand
      (e.g. ``"gbdc base"``) and is split with shell-like quoting.
    - ``contexts`` (list of str): contexts this extractor applies to.
    - ``description`` (str): human-readable description.

``[transformers]``
    Instance transformers: external command-line tools that derive a new instance
    from an existing one. Keyed by transformer name; each entry supports

    - ``tool`` (str): the command gbd runs (see ``tool`` above).
    - ``source`` (list of str): contexts the transformer reads from.
    - ``target`` (list of str): contexts the transformer writes to.
    - ``output_suffix`` (str, optional): overrides the target context's suffix for
      the produced file.
    - ``compress`` (str, optional): compression for the produced file, one of
      ``none``, ``xz``, ``gz``, ``bz2``.
    - ``description`` (str): human-readable description.

Registry blocks (``[extractors]``, ``[transformers]``) may be defined inline,
offloaded to a separate file via ``file = "..."``, or both (inline entries win on a
name clash). When ``file`` is relative, it is resolved against the directory of the
config that references it.

Example
-------

.. code-block:: toml

    [databases]
    paths = ["/data/meta.db", "/data/features.db"]

    [contexts]
    default = "cnf"
    cnf = { suffix = ".cnf", idfunc = "cnf_hash", description = "DIMACS CNF" }

    [extractors]
    # Inline entry ...
    base = { tool = "gbdc base", contexts = ["cnf"], description = "Base features." }
    # ... or offload the rest to a separate file (inline entries win on a clash):
    file = "my_extractors.toml"

    [transformers]
    sanitise = { tool = "gbdc sanitize", source = ["cnf"], target = ["sancnf"], output_suffix = ".sanitized.cnf", compress = "xz", description = "Sanitise CNF files." }
"""

import os
import importlib.resources as pkg_resources

try:  # Python 3.11+
    import tomllib as _toml

    def _read_toml(fp):
        return _toml.load(fp)

except ModuleNotFoundError:  # Python < 3.11
    import tomli as _toml

    def _read_toml(fp):
        return _toml.load(fp)


### Top-level tables that identify a file as a GBD configuration
_CONFIG_TABLES = ("databases", "contexts", "extractors", "transformers")
_REGISTRY_BLOCKS = ("extractors", "transformers")


def _load_toml(path: str) -> dict:
    with open(path, "rb") as f:
        return _read_toml(f)


def _split_paths(value: str) -> list:
    return [p for p in (value or "").split(os.pathsep) if p]


def _named_tables(mapping: dict) -> dict:
    """Keep only the ``name -> table`` entries of a mapping."""
    return {name: value for name, value in mapping.items() if isinstance(value, dict)}


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge ``override`` onto ``base`` (override wins on conflicts)."""
    merged = dict(base)
    for key, value in override.items():
        if isinstance(merged.get(key), dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def is_config_file(path: str) -> bool:
    """True if ``path`` is a single existing file that parses as a GBD TOML config
    (i.e. contains a known top-level table). A colon-separated list is legacy."""
    if not path or os.pathsep in path or not os.path.isfile(path):
        return False
    try:
        return any(table in _load_toml(path) for table in _CONFIG_TABLES)
    except Exception:
        return False


def resolve_sources(cli_db=None):
    """Resolve the ordered configuration layers and legacy database paths.

    Layers are returned low-to-high precedence and merged onto the bundled
    defaults: the ``GBD`` env config, then a ``-d/--db`` config. A legacy
    ``-d/--db`` list (or, as a last resort, ``GBD_DB``) provides the databases
    when no config layer does.

    Returns:
        tuple[list[str], list[str]]: ``(config_layers, db_paths)``.
    """
    layers, db_paths = [], []
    gbd_env = os.environ.get("GBD")
    if gbd_env and is_config_file(gbd_env):
        layers.append(gbd_env)
    if cli_db and is_config_file(cli_db):
        layers.append(cli_db)
    elif cli_db:
        db_paths = _split_paths(cli_db)
    elif not layers:
        db_paths = _split_paths(os.environ.get("GBD_DB"))
    return layers, db_paths


def _load_defaults() -> dict:
    with pkg_resources.files("gbd_core").joinpath("default_config.toml").open("rb") as f:
        return _read_toml(f)


def _resolve_block(raw: dict, block: str, base_dir: str) -> dict:
    """Return a registry block's ``name -> table`` entries, resolving an optional
    ``file`` reference (inline entries win over referenced ones on a name clash)."""
    section = raw.get(block)
    if not isinstance(section, dict):
        return {}
    inline = _named_tables({k: v for k, v in section.items() if k != "file"})
    ref = section.get("file")
    if not ref:
        return inline
    path = ref if os.path.isabs(ref) else os.path.join(base_dir, ref)
    external = _load_toml(path)
    return {**_named_tables(external.get(block, external)), **inline}


def _normalize(raw: dict, base_dir: str) -> dict:
    """Return a copy of a parsed config with registry ``file`` references resolved."""
    return {**raw, **{b: _resolve_block(raw, b, base_dir) for b in _REGISTRY_BLOCKS if b in raw}}


class GbdConfig:
    """Merged view of the GBD configuration (bundled defaults + user config layers)."""

    def __init__(self, config_paths=None):
        if isinstance(config_paths, str):
            config_paths = [config_paths]
        self.config_paths = list(config_paths or [])
        merged = _load_defaults()
        for path in self.config_paths:
            merged = _deep_merge(merged, _normalize(_load_toml(path), os.path.dirname(os.path.abspath(path))))
        self.raw = merged

    def _section(self, name: str) -> dict:
        section = self.raw.get(name, {})
        return section if isinstance(section, dict) else {}

    @property
    def databases(self) -> list:
        """Database paths declared in the config (``[databases] paths = [...]``)."""
        return list(self._section("databases").get("paths", []))

    @property
    def contexts(self) -> dict:
        """The ``[contexts]`` table (including the ``default`` key, if present)."""
        return self._section("contexts")

    @property
    def extractors(self) -> dict:
        """Registered extractors: ``name -> definition`` (file references resolved)."""
        return self._section("extractors")

    @property
    def transformers(self) -> dict:
        """Registered transformers: ``name -> definition`` (file references resolved)."""
        return self._section("transformers")


def default_config() -> GbdConfig:
    """The configuration from the environment: bundled defaults plus the ``GBD``
    config layer when it points to a valid config file."""
    return GbdConfig(resolve_sources()[0])