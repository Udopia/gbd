# GBD Benchmark Database

[![DOI](https://zenodo.org/badge/141396410.svg)](https://zenodo.org/doi/10.5281/zenodo.10213943)

GBD Benchmark Database (GBD) is about bridging the gap between research on SAT algorithms and data science.

## GBD has three interfaces

- Command-line interface `gbd`
- Web interface `gbd serve`
- Python interface `gbd_core.api.GBD`

## GBD solves several problems

- benchmark instance identification and identification of equivalence classes of benchmark instances (gbdhash, isohash, instance family, ...)
- distribution of benchmark instances and benchmark meta-data
- a simple query language to provide access to filtered sets of benchmark instances and instance features
- initialization and maintenance of instance feature databases (meta.db, base.db, gate.db, ...)
- transformation algorithms for benchmark instances such as instance sanitization or transformation of cnf to k-isp instances
- keeping track of contexts (cnf, sanitized-cnf, k-isp, ...) and relations of instances between contexts

## Programming Language

- Python 3
- SQLite

## Installation

- `pip install gbd-tools`

## Configuration

- fetch a database, e.g., [https://benchmark-database.de/getdatabase/meta.db](https://benchmark-database.de/getdatabase/meta.db)
- `export GBD_DB=[path/to/database1]:[path/to/database2:..]` (and put it in your .bashrc)
- test command-line interface with commands `gbd info` and `gbd --help`

## GBD Python Interface

```Python
from gbd_core.api import GBD
with GBD(['path/to/database1', 'path/to/database2', ..] as gbd:
    df = gbd.query("family = hardware-bmc", resolve=['verified-result', 'runtime-kissat'])
```

## GBD Server

This runs under [https://benchmark-database.de/](https://benchmark-database.de/).
The command is available in gbd-tools: `gbd serve --help`

## GBD Command-Line Interface

### gbd get

We assume [https://benchmark-database.de/getdatabase/meta.db](meta.db) is in your gbd path `GBD_DB`.

Get list of benchmark instances in database:

`gbd get`

Get list of benchmark instances including some meta-data

`gbd get -r result family`

Filter for specific benchmark instances with gbd-query

`gbd get "family = hardware-bmc" -r filename`

### gbd init

We assume you installed the python extension module [`gdbc`](https://github.com/Udopia/gbdc).

All initialization routines can run in parallel and per-process ressource limits can be set.
See `gbd init --help` for more info.

#### gbd init local

To initialize a database with local paths to your own benchmarks:

`gbd -d my.db init local [path/to/benchmarks]`

After that in my.db, the features local and filename exist and are associated with their corresponding gbd-hash:

`gbd -d my.db get -r local filename`

#### gbd init isohash

To identify isomorphic instances (approximately by the hash of the sorted degree-sequence of their graph representation):

`gbd -d my.db init isohash`

After that in my.db, instances can be grouped by their isohash:

`gbd -d my.db get -r local filename -g isohash`

#### gbd init base, gbd init gate

`gbd -d my.db:mybase.db init --target mybase base`

`gbd -d my.db:mygate.db init --target mygate gate`
