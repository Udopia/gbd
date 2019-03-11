# gbd (Global Benchmark Database)

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/ce00ab947c6341dca3001607d4ca99f3)](https://app.codacy.com/app/luca_springer/gbd?utm_source=github.com&utm_medium=referral&utm_content=Weitspringer/gbd&utm_campaign=Badge_Grade_Settings)

A tool for collaborative benchmark meta-data management

### Programming Language
Python 3

### Python Packages (Requirements)
- tatsu
- setuptools
- flask

### Getting Started
- setup python3
- setup pip and install required packages
- clone repository
- make sure path in gbd.py is correct (default: #!/usr/bin/python3)

### Usage
By default gbd works with a local database *local.db* which (if not present) is created in the current working directory. To use another database it can be specified with a commandline parameter.

##### Help on basic commands
	./gbd.py -h

##### Help on specific command
	./gbd.py [command] -h

##### Initialize local database
	./gbd.py init [path]

### Documenation
GBD was initially presented at the Pragmatics of SAT (POS) Workshop 2018 hosted at FLoC 2018 in Oxford, UK. Thus, two resources can now be used as documentation of the system. 

###### doc/
The directory contains the Latex source of the original paper published at POS 2018.

###### presentation/ 
The directory contains the Latex source of the presentation slides as presented at POS 2018.
