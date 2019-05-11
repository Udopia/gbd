import os
import sys
from os.path import join

from flask import json

gbd_dir = '.gbd'
config_dir = 'conf'
config_file = 'conf.json'
db_dir = 'db'
db_file = 'local.db'
home = os.environ['HOME']
sys_dir = join(home, gbd_dir)


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    print("Installing config data...")
    if not os.path.isdir(sys_dir):
        os.mkdir(sys_dir)
    config_path = join(sys_dir, config_dir)
    if not os.path.isdir(config_path):
        os.mkdir(config_path)
    config = os.path.join(config_path, config_file)
    open(config, 'w').close()
    f = open(config, 'w')
    f.write('{}\n'.format(json.dumps({'{}'.format(config_dir): '{}'.format(config)})))
    f.close()
    local_db_path = join(sys_dir, db_dir)
    if not os.path.isdir(local_db_path):
        os.mkdir(local_db_path)
    db = os.path.join(local_db_path, db_file)
    f = open(db, 'w')
    f.close()


if __name__ == "__main__":
    main()