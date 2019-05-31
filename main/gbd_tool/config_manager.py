import os
from os.path import join, isfile

from flask import json

standard_config_file = 'conf.json'
custom_config_file = 'custom_conf.json'
db_file = 'local.db'
db_key = 'database'


class ConfigManager:
    def __init__(self, config_path, json_file=None):
        self.config_dir = config_path
        self.config_keys = [db_key]
        if json_file is not None:
            self.make_custom_configuration(json_file)
            self.custom_exists = True
        else:
            self.make_standard_configuration()
            self.custom_exists = False

    def make_standard_configuration(self):
        if not os.path.isdir(self.config_dir):
            os.mkdir(self.config_dir)
        open(join(self.config_dir, standard_config_file), 'w').close()
        f = open(join(self.config_dir, standard_config_file), 'w')
        f.write('{}\n'.format(json.dumps({db_key: join(self.config_dir, db_file)})))
        f.close()

    # Absolute path for custom config file is needed
    def make_custom_configuration(self, json_file):
        if not os.path.isdir(self.config_dir):
            os.mkdir(self.config_dir)
        print('Configuring from {}'.format(json_file))
        if isfile(json_file):
            if self.check_config(json_file):
                custom = open(json_file, 'r')
                content = custom.read()
                custom.close()

                config = open(join(self.config_dir, custom_config_file), 'w')
                config.write(content)
                config.close()
            else:
                raise ValueError('Given json config file lacks keys')
        else:
            raise ValueError('Given config file not found')

    def get_config_file_path(self):
        if self.custom_exists:
            return join(self.config_dir, custom_config_file)
        else:
            return join(self.config_dir, standard_config_file)

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
