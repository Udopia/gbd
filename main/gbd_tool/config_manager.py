import os
from os.path import join, isfile

from flask import json

sys_dir = '/etc'
gbd_dir = join(sys_dir, 'gbd')
config_file = 'conf.json'
db_file = 'local.db'
db_key = 'database'


class ConfigManager:
    def __init__(self, database_path=None):
        if database_path is not None:
            if self.database_path is None:
                self.database_path = database_path
        else:
            self.database_path = db_file

    def make_standard_configuration(self):
        if not os.path.isdir(gbd_dir):
            os.mkdir(gbd_dir)
        if not isfile(join(gbd_dir, db_file)):
            open(join(gbd_dir, db_file), 'w').close()
        open(join(gbd_dir, config_file), 'w').close()
        f = open(join(gbd_dir, config_file), 'w')
        f.write('{}\n'.format(json.dumps({db_key: join(gbd_dir, db_file)})))
        f.close()

    def get_config_file_path(self):
        return join(gbd_dir, config_file)

    def get_config_dictionary(self):
        f = open(self.get_config_file_path(), 'r')
        return json.loads(f.read())

    def get_database_path(self):
        config_dict = self.get_config_dictionary()
        return config_dict.get(db_key)
