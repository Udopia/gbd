# gbd (Global Benchmark Database)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/38208424784e4789a683bd597d58081b)](https://www.codacy.com/app/luca_springer/gbd?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=Weitspringer/gbd&amp;utm_campaign=Badge_Grade)

## Installation
### Programming Language
Python 3

### Getting Started
- setup python3
- setup pip (pip3) and install required packages
    ```console
	    pip3 install flask_limiter global-benchmark-database-tool
	```
- clone repository
- make sure path for python 3 in cli.py is correct
  (default: #!/usr/bin/python3)
- make sure Python was compiled with SSL support
- create an executable link like so ```ln -s $respository/cli.py $bin/gbd```
- obtain a database from https://baldur.iti.kit.edu/gbd/ and safe them under /path/to/db/file.db
- execute ```export GBD_DB=/path/to/db/file.db``` and put it in your .bashrc or somewhere else
- run ```gbd init /path/to/cnf``` in order to reinitialize your benchmarks table
- run ```gbd get -r benchmarks``` to test the system

## Usage
- By default GBD works with a local database *local.db* which (if not present) is created in the current working directory. To use another database it can be specified with a commandline parameter
- After setting up the GBD it can be used either for personal purposes or public usage by starting the shell script:
	```console
	sh run_server.sh
	```
- Flask will run at localhost:5000 by default, but this is only recommended for deployment. For changes modify *server.sh*
- Fetching data from other databases is planned

### Help on basic commands
	./cli.py -h

### Help on specific command
	./cli.py [command] -h

### Initialize local database
	./cli.py init [path]

## Documenation
GBD was initially presented at the Pragmatics of SAT (POS) Workshop 2018 hosted at FLoC 2018 in Oxford, UK. Thus, two resources can now be used as documentation of the system. 

### doc/
The directory contains the Latex source of the original paper published at POS 2018.

### presentation/ 
The directory contains the Latex source of the presentation slides as presented at POS 2018.
