#!/usr/bin/python3
# -*- coding: utf-8 -*-
import os
import sqlite3
from urllib.error import URLError

from main.core.database import groups, tags, search
from main.core import import_data

from main.core.database.db import Database
from main.core.hashing.gbd_hash import gbd_hash
from main.core.http_client import post_request
from main.core.util import eprint, read_hashes, confirm
from os.path import realpath, dirname, join

local_db_path = join(dirname(realpath(__file__)), 'local.db')
DEFAULT_DATABASE = os.environ.get('GBD_DB', local_db_path)


def hash_path(path):
    return gbd_hash(path)


def import_file(database, path, key, source, target):
    with Database(database) as database:
        import_data.import_csv(database, path, key, source, target)


def init_database(database, path=None):
    if path is not None:
        tags.remove_benchmarks(database)
        tags.register_benchmarks(database, path)
    else:
        Database(database)


# TODO: refactor
# entry for modify command
def group(args):
    if args.name.startswith("__"):
        eprint("Names starting with '__' are reserved for system tables")
        return
    with Database(args.db) as database:
        if args.name in groups.reflect(database) and not args.remove and not args.clear:
            eprint("Group {} does already exist".format(args.name))
        elif not args.remove and not args.clear:
            eprint("Adding or modifying group '{}', unique {}, type {}, default-value {}".format(args.name, args.unique
                                                                                                 is not None,
                                                                                               args.type, args.unique))
            groups.add(database, args.name, args.unique is not None, args.type, args.unique)
            return
        if not (args.name in groups.reflect(database)):
            eprint("Group '{}' does not exist".format(args.name))
            return
        if args.remove and confirm("Delete group '{}'?".format(args.name)):
            groups.remove(database, args.name)
        else:
            if args.clear and confirm("Clear group '{}'?".format(args.name)):
                groups.clear(database, args.name)
        return


# entry for query command
def query_search(database, query=None, union=None, intersection=None):
    with Database(database) as database:
        try:
            hashes = search.find_hashes(database, query)
        except sqlite3.OperationalError:
            raise ValueError("Cannot open database file")
        if union:
            inp = read_hashes()
            hashes.update(inp)
        elif intersection:
            inp = read_hashes()
            hashes.intersection_update(inp)
        return hashes


def query_request(host, query, useragent):
    try:
        post_request("{}/query".format(host), {'query': query}, {'User-Agent': useragent})
    except URLError:
        raise ValueError('Cannot send request to host')


# TODO: refactor
# associate a tag with a hash-value
def cli_tag(args):
    hashes = read_hashes()
    with Database(args.db) as database:
        if args.remove and (args.force or confirm("Delete tag '{}' from '{}'?".format(args.value, args.name))):
            for hash in hashes:
                tags.remove_tag(database, args.name, args.value, hash)
        else:
            for hash in hashes:
                tags.add_tag(database, args.name, args.value, hash, args.force)


def resolve(database, hashes, group_names, pattern, collapse):
    with Database(database) as database:
        result = []
        for hash in hashes:
            out = []
            for name in group_names:
                resultset = sorted(search.resolve(database, name, hash))
                resultset = [str(element) for element in resultset]
                if name == 'benchmarks' and pattern is not None:
                    res = [k for k in resultset if pattern in k]
                    resultset = res
                if len(resultset) > 0:
                    if collapse:
                        out.append(resultset[0])
                    else:
                        out.append(' '.join(resultset))
            result.append(out)
        return result


# TODO: refactor
def cli_reflection(args):
    with Database(args.db) as database:
        if args.name is not None:
            if args.values:
                print(*groups.reflect_tags(database, args.name), sep='\n')
            else:
                print('name: {}'.format(args.name))
                print('type: {}'.format(groups.reflect_type(database, args.name)))
                print('uniqueness: {}'.format(groups.reflect_unique(database, args.name)))
                print('default value: {}'.format(groups.reflect_default(database, args.name)))
                print('number of entries: {}'.format(*groups.reflect_size(database, args.name)))
        else:
            print("DB '{}' was created with version: {} and HASH version: {}".format(args.db, database.get_version(),
                                                                                     database.get_hash_version()))
            print("Found tables:")
            print(*groups.reflect(database))
