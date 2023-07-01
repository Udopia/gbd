##!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import unittest
import sqlite3

from gbd_core.query import GBDQuery
from gbd_core.database import Database, DatabaseException
from gbd_core.schema import Schema
from tests import util

class DatabaseTestCase(unittest.TestCase):

    feat = "nonunique_feature"
    val1 = "value1"
    val2 = "value2"

    def setUp(self) -> None:
        self.file = util.get_random_unique_filename('test', '.db')
        sqlite3.connect(self.file).close()
        self.name = Schema.dbname_from_path(self.file)
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
        qb = GBDQuery(self.db, "{}={}".format(feat, val))
        q = qb.build_query()
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
    def test_nonunique_feature_delete(self):
        self.db.delete_feature(self.feat)
        self.assertRaises(DatabaseException, self.db.find, self.feat)