# Global Benchmark Database (GBD)

[![Tests](https://github.com/Udopia/gbd/actions/workflows/tests.yml/badge.svg)](https://github.com/Udopia/gbd/actions/workflows/tests.yml)
[![Paper DOI](https://img.shields.io/badge/DOI-10.4230%2FLIPIcs.SAT.2024.18-blue)](https://doi.org/10.4230/LIPIcs.SAT.2024.18)
[![Zenodo archive v4.7.0](https://img.shields.io/badge/Zenodo%20archive-v4.7.0-blue?logo=zenodo)](https://doi.org/10.5281/zenodo.10213944)

GBD is a comprehensive suite of tools for provisioning and sustainably maintaining benchmark instances and their metadata for empirical research on hard algorithmic problem classes.
For an introduction to the GBD concept, the underlying data model, and specific use cases, please refer to our [2024 SAT Tool Paper](https://doi.org/10.4230/LIPIcs.SAT.2024.18).

## GBD contributes data to your algorithmic evaluations

GBD provides benchmark instance identifiers, feature extractors, and instance transformers for hard algorithmic problem domains, now including propositional satisfiability (SAT) and optimization (MaxSAT), and pseudo-Boolean optimization (PBO).

## GBD solves several problems

- benchmark instance identification
- identification of equivalence classes of benchmark instances
- distribution of benchmark instances and benchmark metadata
- initialization and maintenance of instance feature databases
- transformation algorithms for benchmark instances

GBD provides an extensible set of problem domains, feature extractors, and instance transformers.
For a description of those currently supported, see the [GBDC documentation](https://udopia.github.io/gbdc/doc/Index.html).
GBDC provides GBD's performance-critical code (written in C++) as standalone command-line tools (feature extractors and instance transformers) that gbd invokes as external processes. It is maintained in a separate [repository](https://github.com/Udopia/gbdc).
Extractors and transformers are registered in gbd's configuration, so you can also write and register your own tools.

## Installation and Configuration

- Run `pip install gbd-tools`
- Run `pip install 'gbd-tools[gbdc]'` on supported platforms to also install the GBDC feature extractors and instance transformers used by `gbd init` and `gbd transform`
- Obtain a GBD database, e.g. download [https://benchmark-database.de/getdatabase/meta.db](https://benchmark-database.de/getdatabase/meta.db).
- Register your databases via the environment: `export GBD_DB=path/to/database1:path/to/database2`.
- Alternatively, register a central TOML configuration file via `export GBD=path/to/gbd.toml`. It can declare databases, contexts, extractors, and transformers. When set, it takes precedence over `GBD_DB`; a `-d/--db` argument (a database list or a config file) overrides both.
- Test the command line interface with the `gbd info` and `gbd --help` commands.

## GBD Interfaces

GBD provides the command-line tool `gbd`, the web interface `gbd serve`, and the Python interface `gbd_core.api.GBD`.

### GBD Command-Line Interface

Central commands in gbd are those for data access `gbd get` and database initialization `gbd init`.
See `gbd --help` for more commands.
Once a database is registered (via `GBD_DB`, a `GBD` config file, or `-d/--db`), the `gbd get` command can be used to access data.
See `gbd get --help` for more information.
`gbd init` provides access to registered feature extractors, such as the standalone tools provided by `gbdc`, and `gbd transform` applies registered instance transformers.
All initialization routines can be run in parallel, and resource limits can be set per process.
See `gbd init --help` for more information.

### GBD Server

The GBD server can be started locally with gbd serve. Our instance of the GBD server is hosted at [https://benchmark-database.de/](https://benchmark-database.de/).
You can download benchmark instances and prebuilt feature databases from there.

### GBD Python Interface

The GBD Python interface is used by all programs in the GBD ecosystem. Important here is the query command, which returns GBD data in the form of a Polars dataframe for further analysis, as shown in the following example.

```Python
from gbd_core.api import GBD
with GBD(['path/to/database1', 'path/to/database2', ..] as gbd:
    df = gbd.query("family = hardware-bmc", resolve=['verified-result', 'runtime-kissat'])
```

Scripts and use cases of GBD's Python interface are available on [https://udopia.github.io/gbdeval/](https://udopia.github.io/gbdeval/).
The [evaluation demo](https://udopia.github.io/gbdeval/demo_evaluation.html) demonstrates portfolio analysis and subsequent category-wise performance evaluation using the 2023 SAT competition data.
The [prediction demo](https://udopia.github.io/gbdeval/demo_prediction.html) demonstrates category prediction from instance features and subsequent feature importance evaluation.

## Release Notes

### GBD 5.2

GBD 5.2 adds the `gbd interactive` command, which opens an IPython shell with the result of a query available as a Polars dataframe for exploratory analysis (install via `pip install 'gbd-tools[interactive]'`).
Database initialization is also considerably faster: feature values are now written in batches, which greatly reduces the time spent populating large feature databases.
Both features are based on contributions by Christoph Jabs ([@chrjabs](https://github.com/chrjabs), pull requests [#32](https://github.com/Udopia/gbd/pull/32) and [#39](https://github.com/Udopia/gbd/pull/39)), imported and adapted to the Polars-based interface and the configuration-driven architecture introduced in 5.1.

### GBD 5.1

GBD 5.1 decouples gbd from GBDC: the performance-critical feature extractors and instance transformers are now standalone command-line tools that gbd invokes as external processes, rather than a hard Python dependency.
Extractors, transformers, contexts, and databases are declared in a TOML configuration, so you can register and use your own tools.
A central configuration file can be provided via the new `GBD` environment variable, which takes precedence over `GBD_DB`.

### GBD 5.0

In addition to several bug fixes and performance improvements, GBD 5.0 no longer depends on Pandas for its interface module.
This simplifies installation and use in various environments.
The faster, more lightweight Polars library is now used for dataframes instead.
Therefore, upgrading to GBD 5.0 requires existing code to be adapted to use Polars dataframes, or Polars dataframes to be explicitly converted to Pandas dataframes (e.g. via df.to_pandas()).

