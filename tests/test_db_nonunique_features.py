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
    feat = "nonunique_feature"
    val1 = "value1"
    val2 = "value2"

    def setUp(self) -> None:
        self.db = Database([self.file], verbose=False)
        self.db.create_feature(self.feat, default_value=None)
        self.db.set_values(self.feat, self.val1, ["a", "b", "c"])
        self.db.set_values(self.feat, self.val2, ["a", "b", "c"])
        return super().setUp()

    def tearDown(self) -> None:
        if os.path.exists(self.file):
            os.remove(self.file)
        return super().tearDown()

    def query(self, feat, val):
        qb = GBDQuery(self.db)
        q = qb.build_query("{}={}".format(feat, val))
        return [ hash for (hash, ) in self.db.query(q) ]

    def dump(self):
        import sqlite3
        conn = sqlite3.connect(self.file)
        for line in conn.iterdump():
            print(line)
        conn.close()

    # Test that the feature values are initialized correctly in test setup
    def test_feature_values_exist(self):
        res = self.query(self.feat, self.val1)
        self.assertEqual(len(res), 3)
        self.assertSetEqual(set(res), set(["a", "b", "c"]))
        res = self.query(self.feat, self.val2)
        self.assertEqual(len(res), 3)
        self.assertSetEqual(set(res), set(["a", "b", "c"]))

    # Delete specific hash-value pair and check that it is gone and the others are still there
    def test_feature_values_delete_hash_value(self):
        self.db.delete(self.feat, [ self.val1 ], ["a"])
        res = self.query(self.feat, self.val1)
        self.assertEqual(len(res), 2)
        self.assertSetEqual(set(res), set(["b", "c"]))
        res = self.query(self.feat, self.val2)
        self.assertEqual(len(res), 3)
        self.assertSetEqual(set(res), set(["a", "b", "c"]))

    # Delete specific hash and check that it is gone and the others are still there
    def test_feature_values_delete_hash(self):
        self.db.delete(self.feat, [ ], ["a"])
        res = self.query(self.feat, self.val1)
        self.assertEqual(len(res), 2)
        self.assertSetEqual(set(res), set(["b", "c"]))
        res = self.query(self.feat, self.val2)
        self.assertEqual(len(res), 2)
        self.assertSetEqual(set(res), set(["b", "c"]))
        res = self.query(self.feat, "None")
        self.assertEqual(len(res), 1)
        self.assertSetEqual(set(res), set(["a"]))

    # Delete specific value and check that it is gone and the others are still there
    def test_feature_values_delete_value(self):
        self.db.delete(self.feat, [ self.val1 ], [ ])
        res = self.query(self.feat, self.val1)
        self.assertEqual(len(res), 0)
        res = self.query(self.feat, self.val2)
        self.assertEqual(len(res), 3)
        self.assertSetEqual(set(res), set([ "a", "b", "c" ]))

    # Delete feature
    def test_unique_feature_delete(self):
        self.db.delete_feature(self.feat)
        self.assertFalse(self.db.fexists(self.feat))