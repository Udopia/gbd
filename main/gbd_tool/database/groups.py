# Global Benchmark Database (GBD)
# Copyright (C) 2019 Markus Iser, Luca Springer, Karlsruhe Institute of Technology (KIT)
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

def add(database, cat, unique=False, type='text', default=None):
    ustr = "UNIQUE" if unique else ""
    dstr = "DEFAULT \"{}\"".format(default) if default is not None else ""
    database.submit(
        'CREATE TABLE IF NOT EXISTS {} (hash TEXT {} NOT NULL, value {} NOT NULL {})'.format(cat, ustr, type, dstr))
    if default is not None:
        database.submit('INSERT OR IGNORE INTO {} (hash) SELECT hash FROM benchmarks'.format(cat))


def remove(database, cat):
    database.submit('DROP TABLE IF EXISTS {}'.format(cat))


def clear(database, cat):
    database.submit('DELETE FROM {}'.format(cat))


def reflect(database, cat=None):
    if cat is None:
        lst = database.query("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        groups = [x[0] for x in lst]
        return groups
    else:
        lst = database.query("PRAGMA table_info({})".format(cat))
        columns = ('index', 'name', 'type', 'notnull', 'default_value', 'pk')
        table_infos = [dict(zip(columns, values)) for values in lst]

        lst = database.query("PRAGMA index_list({})".format(cat))
        columns = ('seq', 'name', 'unique', 'origin', 'partial')
        index_list = [dict(zip(columns, values)) for values in lst]

        # create key default
        for info in table_infos:
            info['unique'] = 0

        # determine unique columns
        for values in index_list:
            tup = database.query("PRAGMA index_info({})".format(values['name']))
            columns = ('index_rank', 'table_rank', 'name')
            index_info = dict(zip(columns, tup[0]))
            colid = index_info['table_rank']
            table_infos[colid]['unique'] = values['unique']

        return table_infos


def reflect_tags(database, cat):
    return database.value_query('SELECT DISTINCT value FROM {}'.format(cat))


def reflect_size(database, cat):
    return database.value_query('SELECT count(*) FROM {}'.format(cat))


def reflect_unique(database, cat):
    info = reflect(database, cat)
    return info[0]['unique']


def reflect_default(database, cat):
    info = reflect(database, cat)
    return info[1]['default_value']


def reflect_type(database, cat):
    info = reflect(database, cat)
    return info[1]['type']
