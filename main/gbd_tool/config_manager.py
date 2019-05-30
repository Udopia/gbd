import os
from os.path import join, isfile

from flask import json

sys_dir = '/etc'
gbd_dir = join(sys_dir, 'gbd')
standard_config_file = 'conf.json'
custom_config_file = 'custom_conf.json'
db_file = 'local.db'
db_key = 'database'
custom_exists = False


class ConfigManager:
    def __init__(self, json_file=None):
        if json_file is not None:
            self.make_custom_configuration(json_file)
            global custom_exists
            custom_exists = True
        else:
            self.database_path = db_file
        self.config_keys = [db_key]

    @staticmethod
    def make_standard_configuration():
        if not os.path.isdir(gbd_dir):
            os.mkdir(gbd_dir)
        if not isfile(join(gbd_dir, db_file)):
            open(join(gbd_dir, db_file), 'w').close()
        open(join(gbd_dir, standard_config_file), 'w').close()
        f = open(join(gbd_dir, standard_config_file), 'w')
        f.write('{}\n'.format(json.dumps({db_key: join(gbd_dir, db_file)})))
        f.close()

    # Absolute path for custom config file is needed
    def make_custom_configuration(self, json_file):
        if not os.path.isdir(gbd_dir):
            os.mkdir(gbd_dir)
        if isfile(json_file):
            if self.check_config(json_file):
                custom = open(json_file, 'r')
                content = custom.read()
                custom.close()

                config = open(join(gbd_dir, custom_config_file), 'w')
                config.write(content)
                config.close()
            else:
                raise ValueError('Given json config file lacks keys')
        else:
            raise ValueError('Given config file not found')

    @staticmethod
    def get_config_file_path():
        if custom_exists:
            return join(gbd_dir, custom_config_file)
        else:
            return join(gbd_dir, standard_config_file)

    def get_config_dictionary(self):
        f = open(self.get_config_file_path(), 'r')
        return json.loads(f.read())

    def get_database_path(self):
        config_dict = self.get_config_dictionary()
        return config_dict.get(db_key)

    def check_config(self, config_file):
        f = open(config_file, 'r')
        dictionary = json.loads(f.read())
        try:
            for key in self.config_keys:
                dictionary.get(key)
        except KeyError:
            f.close()
            return False
        f.close()
        return True
