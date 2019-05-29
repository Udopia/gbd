import os
from os.path import join, isfile

from flask import json

sys_dir = '/etc'
gbd_dir = join(sys_dir, 'gbd')
config_file = 'conf.json'
db_file = 'local.db'
db_key = 'database'


def make_standard_configuration():
    if not os.path.isdir(join(gbd_dir)):
        os.mkdir(gbd_dir)
    if not isfile(db_file):
        open(db_file, 'w').close()
    open(config_file, 'w').close()
    f = open(config_file, 'w')
    f.write('{}\n'.format(json.dumps({db_key: join(gbd_dir, db_file)})))
    f.close()


def get_config_file_path():
    return join(gbd_dir, config_file)


def get_config_dictionary():
    f = open(get_config_file_path(), 'r')
    return json.loads(f.read())


def get_database_path():
    config_dict = get_config_dictionary()
    return config_dict.get(db_key)
