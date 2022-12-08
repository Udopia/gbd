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
        self.db = Database([self.file], verbose=True)
        return super().setUp()

    def tearDown(self) -> None:
        if os.path.exists(self.file):
            os.remove(self.file)
        return super().tearDown()

    def test_create_db(self):
        self.assertTrue(Schema.is_database(self.file))
        self.assertEqual(len(self.db.get_databases()), 1)
        self.assertTrue(self.db.dexists(self.name))
        
    def test_create_unique_feature(self):
        FEAT = "featA"
        self.db.create_feature(FEAT, default_value="empty")
        self.assertIn(FEAT, self.db.get_features())
        self.assertIn("features", self.db.get_tables())
        self.assertEqual(self.db.ftable(FEAT, full=False), "features")
        self.assertEqual(self.db.fcolumn(FEAT), FEAT)
        self.assertEqual(self.db.fdefault(FEAT), "empty")
        self.assertFalse(self.db.fvirtual(FEAT))
        self.assertEqual(self.db.fcontext(FEAT), "cnf")
        self.assertEqual(self.db.fdatabase(FEAT), self.name)
        
    def test_create_nonunique_feature(self):
        FEAT = "featB"
        self.db.create_feature(FEAT, default_value=None)
        self.assertIn(FEAT, self.db.get_features())
        self.assertIn("features", self.db.get_tables())
        self.assertEqual(self.db.ftable(FEAT, full=False), FEAT)
        self.assertEqual(self.db.fcolumn(FEAT), "value")
        self.assertEqual(self.db.fdefault(FEAT), None)
        self.assertFalse(self.db.fvirtual(FEAT))
        self.assertEqual(self.db.fcontext(FEAT), "cnf")
        self.assertEqual(self.db.fdatabase(FEAT), self.name)

    def test_unique_feature_values(self):
        FEAT = "featA"
        VAL1 = "val1"
        VAL2 = "val2"
        self.db.create_feature(FEAT, default_value="empty")
        self.db.set_values(FEAT, VAL1, ["a", "b", "c"])
        qb = GBDQuery(self.db)
        q = qb.build_query("{}={}".format(FEAT, VAL1))
        hashes = [ hash for (hash, ) in self.db.query(q) ]
        self.assertEqual(len(hashes), 3)
        self.assertSetEqual(set(hashes), set(["a", "b", "c"]))
        self.db.set_values(FEAT, VAL2, ["a"])
        q = qb.build_query("{}={}".format(FEAT, VAL1))
        hashes = [ hash for (hash, ) in self.db.query(q) ]
        self.assertEqual(len(hashes), 2)
        self.assertSetEqual(set(hashes), set(["b", "c"]))
        q = qb.build_query("{}={}".format(FEAT, VAL2))
        hashes = [ hash for (hash, ) in self.db.query(q) ]
        self.assertEqual(len(hashes), 1)
        self.assertSetEqual(set(hashes), set(["a"]))



if __name__ == '__main__':
    unittest.main()