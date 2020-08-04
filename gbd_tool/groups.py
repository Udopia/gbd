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

from gbd_tool.util import eprint

def add(database, name, unique=False, default=None):
    ustr = "UNIQUE" if unique else ""
    dstr = "DEFAULT \"{}\"".format(default) if default is not None else ""
    database.submit(
        'CREATE TABLE IF NOT EXISTS {} (hash TEXT {} NOT NULL, value TEXT NOT NULL {})'.format(name, ustr, dstr))
    if default is not None:
        database.submit('INSERT OR IGNORE INTO {} (hash) SELECT hash FROM local'.format(name))

def remove(database, name):
    database.submit('DROP TABLE IF EXISTS {}'.format(name))

def clear(database, name):
    database.submit('DELETE FROM {}'.format(name))


def reflect(database, name):
    table_infos = [info.update({'unique': False}) or info for info in database.table_info(name)]
       
    # determine unique columns
    index_list = database.index_list(name)
    for index in [e for e in index_list if e['unique']]:
        col = database.index_info(index['name'])['table_rank']
        table_infos[col]['unique'] = True

    for info in table_infos:
        if info['default_value'] is not None:
            info['default_value'] = info['default_value'].strip('"')
    
    return table_infos


def reflect_tags(database, name):
    return database.value_query('SELECT DISTINCT value FROM {}'.format(name))


def reflect_size(database, name):
    return database.value_query('SELECT count(*) FROM {}'.format(name))


def reflect_unique(database, name):
    info = reflect(database, name)
    return info[0]['unique']


def reflect_default(database, name):
    info = reflect(database, name)
    return info[1]['default_value']
