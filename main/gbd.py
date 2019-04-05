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


def hash_file(path):
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
        return post_request("{}/query".format(host), {'query': query}, {'User-Agent': useragent})
    except URLError:
        raise ValueError('Cannot send request to host')


# associate a tag with a hash-value
def add_tag(database, name, value, hashes, force):
    with Database(database) as database:
        for h in hashes:
            tags.add_tag(database, name, value, h, force)


def remove_tag(database, name, value, hashes):
    with Database(database) as database:
        for h in hashes:
            tags.remove_tag(database, name, value, h)


def resolve(database, hashes, group_names, pattern, collapse):
    with Database(database) as database:
        result = []
        for h in hashes:
            out = []
            for name in group_names:
                resultset = sorted(search.resolve(database, name, h))
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


def get_group_info(database, name):
    if name is not None:
        with Database(database) as database:
            return {'name': name, 'type': groups.reflect_type(database, name),
                    'uniqueness': groups.reflect_unique(database, name),
                    'default': groups.reflect_default(database, name),
                    'entries': groups.reflect_size(database, name)}
    else:
        raise ValueError('No group given')


def get_group_values(database, name):
    if name is not None:
        return query_search(database, '{} like %%%%'.format(name))
    else:
        raise ValueError('No group given')


def get_database_info(database):
    with Database(database) as database:
        return {'name': database, 'version': database.get_version(), 'hash-version': database.get_hash_version(),
                'tables': groups.reflect(database)}
