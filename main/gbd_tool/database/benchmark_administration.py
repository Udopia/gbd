import os
from os.path import isfile

from main.gbd_tool.database import groups
from main.gbd_tool.database.db import Database, DatabaseException
from main.gbd_tool.hashing.gbd_hash import gbd_hash
from main.util.util import eprint


def add_tag(database, cat, tag, hash, force=False):
    info = groups.reflect(database, cat)
    if (info[0]['unique']):
        if force:
            database.submit('REPLACE INTO {} (hash, value) VALUES ("{}", "{}")'.format(cat, hash, tag))
        else:
            res = database.query("SELECT value FROM {} WHERE hash='{}'".format(cat, hash))
            if (len(res) == 0):
                database.submit('INSERT INTO {} (hash, value) VALUES ("{}", "{}")'.format(cat, hash, tag))
            else:
                value = res[0][0]
                if value == info[1]['default_value']:
                    eprint("Overwriting default-value {} with new value {} for hash {}".format(value, tag, hash))
                    database.submit('REPLACE INTO {} (hash, value) VALUES ("{}", "{}")'.format(cat, hash, tag))
                elif value != tag:
                    eprint(
                        "Unable to insert tag ({}, {}) into unique '{}' as a different value is already set: '{}'".format(
                            hash, tag, cat, value))
    else:
        res = database.value_query("SELECT hash FROM {} WHERE hash='{}' AND value='{}'".format(cat, hash, tag))
        if (len(res) == 0):
            database.submit('INSERT INTO {} (hash, value) VALUES ("{}", "{}")'.format(cat, hash, tag))


def remove_tag(database, cat, tag, hash):
    database.submit("DELETE FROM {} WHERE hash='{}' AND value='{}'".format(cat, hash, tag))


def add_benchmark(database, hash, path):
    database.submit('INSERT INTO benchmarks (hash, value) VALUES ("{}", "{}")'.format(hash, path))
    g = groups.reflect(database)
    for group in g:
        info = groups.reflect(database, group)
        dval = info[1]['default_value']
        if (dval is not None):
            database.submit('INSERT OR IGNORE INTO {} (hash) VALUES ("{}")'.format(group, hash))


def remove_benchmarks(db):
    with Database(db) as database:
        paths = database.value_query("SELECT value FROM benchmarks")
        for p in paths:
            if not isfile(p):
                eprint("Problem '{}' not found. Removing...".format(p))
                database.submit("DELETE FROM benchmarks WHERE value='{}'".format(p))


def register_benchmarks(db, benchmark_root):
    for root, dirnames, filenames in os.walk(benchmark_root):
        for filename in filenames:
            path = os.path.join(root, filename)
            eprint('Found {}'.format(path))
            try:
                with Database(db) as database:
                    hashes = database.value_query("SELECT hash FROM benchmarks WHERE value = '{}'".format(path))
                    if len(hashes) is not 0:
                        eprint('Problem {} already hashed'.format(path))
                        continue
                hashvalue = gbd_hash(path)
                with Database(db) as database:
                    add_benchmark(database, hashvalue, path)
            except DatabaseException as e:
                eprint(e)
                return
            except UnicodeDecodeError as e:
                eprint('Skipping file due to decoding error: {}'.format(e))
                continue
