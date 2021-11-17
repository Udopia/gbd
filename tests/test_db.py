#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import sys
sys.path.insert(0, os.path.abspath( os.path.join(os.path.dirname(__file__), '../') ))

import unittest

import gbd_tool
from gbd_tool.util import eprint, make_alnum_ul
from gbd_tool.gbd_api import GBD
from gbd_tool.db import Database

class DatabaseTestCase(unittest.TestCase):

    TDB = "./test.db"
    TDBN = make_alnum_ul(os.path.basename(TDB))

    def test_create_db(self):
        os.remove(self.TDB)
        with Database([self.TDB], verbose=True) as db:
            assert(db.is_database(self.TDB))
            assert(len(db.databases()) == 2)
            eprint(db.features())
            assert(len(db.features()) == 0)
            assert(len(db.tables()) == 0)

    def test_create_feature(self):
        os.remove(self.TDB)
        with Database([self.TDB], verbose=True) as db:
            db.create_feature("A")
            assert("A" in db.features())
            tab = self.TDBN + ".A"
            assert(tab in db.tables())

    def test_create_unique_feature(self):
        os.remove(self.TDB)
        with Database([self.TDB], verbose=True) as db:
            db.create_feature("B", "empty")
            assert("B" in db.features())
            tab = self.TDBN + ".features"
            assert(tab in db.tables())

if __name__ == '__main__':
    unittest.main()