import os
import sqlite3
from os.path import realpath, dirname, join
from urllib.error import URLError

from main.core import import_data
from main.core.database import groups, tags, search
from main.core.database.db import Database
from main.core.hashing.gbd_hash import gbd_hash
from main.core.http_client import post_request

local_db_path = join(dirname(realpath(__file__)), 'local.db')
DEFAULT_DATABASE = os.environ.get('GBD_DB', local_db_path)


def hash_file(path):
    return gbd_hash(path)


def import_file(database, path, key, source, target):
    with Database(database) as database:
        import_data.import_csv(database, path, key, source, target)


def init_database(database, path=None):
    with Database(database) as database:
        if path is not None:
            tags.remove_benchmarks(database)
            tags.register_benchmarks(database, path)
        else:
            Database(database)


def check_group_exists(database, name):
    with Database(database) as database:
        if name in groups.reflect(database):
            return True
        else:
            return False


def add_attribute_group(database, name, type, unique):
    with Database(database) as database:
        groups.add(database, name, unique is not None, type, unique)


def remove_attribute_group(database, name):
    with Database(database) as database:
        groups.remove(database, name)


def clear_group(database, name):
    with Database(database) as database:
        groups.remove(database, name)


def hash_union(hash_list, other_hash_list):
    return hash_list.update(other_hash_list)


def hash_intersection(hash_list, other_hash_list):
    return hash_list.intersection_update(other_hash_list)


# entry for query command
def query_search(database, query=None):
    with Database(database) as database:
        try:
            hashes = search.find_hashes(database, query)
        except sqlite3.OperationalError:
            raise ValueError("Cannot open database file")
        return hashes


def query_request(host, query, useragent):
    try:
        return set(post_request("{}/query".format(host), {'query': query}, {'User-Agent': useragent}))
    except URLError:
        raise ValueError('Cannot send request to host')


# associate a  with a hash-value
def set_attribute(database, name, value, hash_list, force):
    with Database(database) as database:
        for h in hash_list:
            tags.add_tag(database, name, value, h, force)


def remove_attribute(database, name, value, hash_list):
    with Database(database) as database:
        for h in hash_list:
            tags.remove_tag(database, name, value, h)


def resolve(database, hash_list, group_list, pattern=None, collapse=False):
    with Database(database) as database:
        result = []
        for h in hash_list:
            out = []
            for name in group_list:
                if not name.startswith("__"):
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


def resolve_request(host, hashes, group, collapse, pattern, useragent):
    try:
        return set(post_request("{}/resolve".format(host), {'hash': hashes, 'group': group, 'collapse': collapse,
                                                            'pattern': pattern}, {'User-Agent': useragent}))
    except URLError:
        raise ValueError('Cannot send request to host')


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
        return {'name': database, 'version': database.get_version(), 'hash-version': database.get_hash_version()}


def get_all_tables(database):
    with Database(database) as database:
        return groups.reflect(database)
