# gbd (Global Benchmark Database)
A tool for global benchmark management.
Sometimes, it is hard to get a overview about all solutions for different problems and how fast/complex the algorithms 
are and how they can compete with your solution. What if all your benchmark files (.csv) would be managed by a database?
This is what GBD does: Searching for files on your computer (path given), hashes (and resolves) 
them and add schemes for different attributes of benchmarks (clauses, variables, etc.).

## Installation
### Programming Language
Python 3

### Python Packages (Requirements)
- tatsu (install with pip3)
- setuptools
- flask

### Getting Started
- setup python3
- make sure Python was compiled with SSL support
- to get the default configuration up and running, simply type 
    ```console
	    sudo gbd-tool-init
	```
  in your console. If you get an DistributionError, make sure your default python version the shell is using is python 3!
    ```console
     alias python=python3
    ```
  (see more: https://stackoverflow.com/questions/5846167/how-to-change-default-python-version)
- when coding, use package 'gbd_tool' for importing components

## Usage
- By default GBD works with a local sqlite3 database *local.db* which you have to define and give to the parameters of the
  methods in gbd_api. The easiest way is to set following path in the class of your choice:
  local_db_path = join(dirname(realpath(__file__)), 'local.db')
  Important: You have to provide absolute paths to the api, not relative paths!!!
- You can set up your database by using methods in gbd_api.py
- If you want to provide this management system on a server, there will be a package for doing this

## Documenation
GBD was initially presented at the Pragmatics of SAT (POS) Workshop 2018 hosted at FLoC 2018 in Oxford, UK. Thus, two resources can now be used as documentation of the system. 
