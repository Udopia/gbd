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

import os
from os.path import join
from flask import json

config_file = 'conf.json'
db_file = 'local.db'
db_key = 'database'


class ConfigManager:
    def __init__(self, config_path, database=None):
        self.config_dir = config_path
        self.config_keys = [db_key]
        if database is not None:
            self.make_configuration(database)
        else:
            raise AttributeError("No database specified to work with")

    # Absolute path for custom config file is needed
    def make_configuration(self, database):
        if not os.path.isdir(self.config_dir):
            os.mkdir(self.config_dir)
        config = open(join(self.config_dir, config_file), 'w')
        config.write('{}\n'.format(json.dumps({db_key: database})))
        config.close()

    def get_config_file_path(self):
        return join(self.config_dir, config_file)

    def get_config_dictionary(self):
        f = open(self.get_config_file_path(), 'r')
        return json.loads(f.read())

    def get_database_path(self):
        config_dict = self.get_config_dictionary()
        return config_dict.get(db_key)
