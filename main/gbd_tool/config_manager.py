import os
from os.path import join, isfile

from flask import json

sys_dir = '/etc'
gbd_dir = join(sys_dir, '.gbd')
config_dir = 'conf'
config_file = 'conf.json'
config_key = 'config'
db_dir = 'db'
db_file = 'local.db'
db_key = 'database'


def make_standard_configuration():
    if not os.path.isdir(join(gbd_dir)):
        os.mkdir(gbd_dir)
    config_dir_path = join(gbd_dir, config_dir)
    local_db_dir_path = join(gbd_dir, db_dir)
    if not os.path.isdir(local_db_dir_path):
        os.mkdir(local_db_dir_path)
    db = os.path.join(local_db_dir_path, db_file)
    if not isfile(db):
        open(db, 'w').close()
    if not os.path.isdir(config_dir_path):
        os.mkdir(config_dir_path)

    config = os.path.join(config_dir_path, config_file)
    open(config, 'w').close()
    f = open(config, 'w')
    f.write('{}\n'.format(json.dumps({config_key: None,
                                      db_key: db})))
    f.close()


def get_config_file_path():
    return join(gbd_dir, config_dir, config_file)
