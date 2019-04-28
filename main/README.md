# gbd (Global Benchmark Database)

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

## Usage
- By default GBD works with a local sqlite3 database *local.db* which you have to define and give to the parameters of the
  methods in gbd_api. The easiest way is to set following path in the class of your choice:
  local_db_path = join(dirname(realpath(__file__)), 'local.db')
- You can set up your database by using methods in gbd_api.py
- If you want to provide this management system on a server, there will be a package for doing this

## Documenation
GBD was initially presented at the Pragmatics of SAT (POS) Workshop 2018 hosted at FLoC 2018 in Oxford, UK. Thus, two resources can now be used as documentation of the system. 
