# Global Benchmark Database (GBD)
# Copyright (C) 2020 Markus Iser, Karlsruhe Institute of Technology (KIT)
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
        database.submit('''CREATE TRIGGER {}_dval AFTER INSERT ON local BEGIN 
                INSERT INTO {} (hash) VALUES (NEW.hash); END'''.format(name, name))

def remove(database, name):
    database.submit('DROP TABLE IF EXISTS {}'.format(name))
    database.submit('DROP TRIGGER IF EXISTS {}_dval'.format(name))
