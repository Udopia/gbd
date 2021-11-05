#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import sys
sys.path.insert(0, os.path.abspath( os.path.join(os.path.dirname(__file__), '../') ))

import unittest

import gbd_tool
from gbd_tool.util import eprint
from gbd_tool.gbd_api import GBD
from gbd_tool.db import Database

class InitTestCase(unittest.TestCase):

    def test_db(self):
        with Database(["/raid/gbd/meta.db"], verbose=True) as db:
            for table in db.tables():
                eprint(db.table_default_value(table))

if __name__ == '__main__':
    unittest.main()