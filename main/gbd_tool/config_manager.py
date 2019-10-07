import os
from os.path import join, exists

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
        database_path = os.environ.get('GBD_DB', database)
        if not os.path.isdir(self.config_dir):
            os.mkdir(self.config_dir)
        config = open(join(self.config_dir, config_file), 'w')
        config.write('{}\n'.format(json.dumps({db_key: database_path})))
        config.close()

    def get_config_file_path(self):
        return join(self.config_dir, config_file)

    def get_config_dictionary(self):
        f = open(self.get_config_file_path(), 'r')
        return json.loads(f.read())

    def get_database_path(self):
        config_dict = self.get_config_dictionary()
        return config_dict.get(db_key)
