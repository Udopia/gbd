# gbd (Global Benchmark Database)

## Programming Language
Python 3

## Installation

- Setup python3 (with SSL support) and pip3
- Clone Repository
- Install required packages: ```pip3 install -U -r requirements.txt```
- Make sure path for `python3` in `cli.py` is correct (default: ```#!/usr/bin/python3```)
- Create executable link: ```ln -s $respository/cli.py ~/bin/gbd```
- Optional: Download a database, e.g., [https://baldur.iti.kit.edu/gbd/](https://baldur.iti.kit.edu/gbd/roth.db), and safe it under /path/to/db/file.db
- Configure DB Path: ```export GBD_DB=/path/to/db/file.db``` (and put it in your .bashrc)
- Reinitialize paths in "benchmarks" table: ```gbd init /path/to/cnf```

## Test
>   ```gbd get -r benchmarks```

### Using GBD Server
- After getting started, you can use GBD from the command line as explained in the help section
- For starting the server on Linux, run ```sh /server/run_server.sh /path/to/db/file.db```. If no path is given, the script uses the path from ```GBD_DB```
- For starting the server on Windows, run ```\path\to\python3\interpreter \server\server.py -d \path\to\db\file.db```

### Help on basic commands
>	```gbd -h```

### Help on specific command
>	```gbd [command] -h```

## Documenation
GBD was initially presented at the Pragmatics of SAT (POS) Workshop 2018 hosted at FLoC 2018 in Oxford, UK. Thus, two resources can now be used as documentation of the system.

### misc/doc/
The directory contains the Latex source of the original paper published at POS 2018.

### misc/presentation/
The directory contains the Latex source of the presentation slides as presented at POS 2018.
