# Global Benchmark Database (GBD)

[![DOI](https://zenodo.org/badge/141396410.svg)](https://zenodo.org/doi/10.5281/zenodo.10213943)

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
GBDC is a Python extension module for GBD's performance-critical code (written in C++), maintained in a separate [repository](https://github.com/Udopia/gbdc).

## Installation and Configuration

- Run `pip install gbd-tools`
- Run `pip install gbdc` (optional, installation of extension module gbdc)
- Obtain a GBD database, e.g. download [https://benchmark-database.de/getdatabase/meta.db](https://benchmark-database.de/getdatabase/meta.db).
- Configure your environment by registering paths to databases like this `export GBD_DB=path/to/database1:path/to/database2`.
- Test the command line interface with the `gbd info` and `gbd --help` commands.

## GBD Interfaces

GBD provides the command-line tool `gbd`, the web interface `gbd serve`, and the Python interface `gbd_core.api.GBD`.

### GBD Command-Line Interface

Central commands in gbd are those for data access `gbd get` and database initialization `gbd init`.
See `gbd --help` for more commands.
Once a database is registered in the environment variable `GBD_DB`, the `gbd get` command can be used to access data.
See `gbd get --help` for more information.
`gbd init` provides access to registered feature extractors, such as those provided by the `gdbc` extension module.
All initialization routines can be run in parallel, and resource limits can be set per process.
See `gbd init --help` for more information.

### GBD Server

The GBD server can be started locally with gbd serve. Our instance of the GBD server is hosted at [https://benchmark-database.de/](https://benchmark-database.de/).
You can download benchmark instances and prebuilt feature databases from there.

### GBD Python Interface

The GBD Python interface is used by all programs in the GBD ecosystem. Important here is the query command, which returns GBD data in the form of a Pandas dataframe for further analysis, as shown in the following example.

```Python
from gbd_core.api import GBD
with GBD(['path/to/database1', 'path/to/database2', ..] as gbd:
    df = gbd.query("family = hardware-bmc", resolve=['verified-result', 'runtime-kissat'])
```

Scripts and use cases of GBD's Python interface are available on [https://udopia.github.io/gbdeval/](https://udopia.github.io/gbdeval/).
The [evaluation demo](https://udopia.github.io/gbdeval/demo_evaluation.html) demonstrates portfolio analysis and subsequent category-wise performance evaluation using the 2023 SAT competition data.
The [prediction demo](https://udopia.github.io/gbdeval/demo_prediction.html) demonstrates category prediction from instance features and subsequent feature importance evaluation.

