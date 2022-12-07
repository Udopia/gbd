##!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import sys

#gbdroot=os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
#if gbdroot in sys.path:
#    sys.path.remove(gbdroot)
#sys.path.insert(0, gbdroot)
#print(sys.path)

import unittest

from gbd_tool.util import eprint
from gbd_tool.query_builder import GBDQuery
from gbd_tool.db import Database
from gbd_tool.schema import Schema

class DatabaseTestCase(unittest.TestCase):

    file = "test.db"
    name = Schema.dbname_from_path(file)

    def setUp(self) -> None:
        return super().setUp()

    def tearDown(self) -> None:
        if os.path.exists(self.file):
            os.remove(self.file)
        return super().tearDown()

    def test_create_db(self):
        with Database([self.file], verbose=True) as db:
            assert(Schema.is_database(self.file))
            assert(len(db.get_databases()) == 1)
            assert(len(db.get_features()) == 0)
            assert(len(db.get_tables()) == 0)
        with Database([self.file], verbose=True) as db:
            assert(db.dpath(self.name) == self.file)
            assert(db.dmain(self.name))
            assert(len(db.dcontexts(self.name)) == 0)
            assert(len(db.dtables(self.name)) == 0)
            assert(len(db.dviews(self.name)) == 0)



if __name__ == '__main__':
    unittest.main()