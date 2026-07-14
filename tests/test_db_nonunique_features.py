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
        self.db.set_values({self.feat: self.val1}, ["a", "b", "c"])
        self.db.set_values({self.feat: self.val2}, ["a", "b", "c"])
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

    # ---- additional coverage ---------------------------------------------------

    def test_rename_nonunique_feature(self):
        self.db.rename_feature(self.feat, "renamed_feat")
        # old name gone from registry
        self.assertNotIn(self.feat, self.db.get_features())
        self.assertRaises(DatabaseException, self.db.find, self.feat)
        # new name queryable
        qb = GBDQuery(self.db, "renamed_feat={}".format(self.val1))
        q = qb.build_query()
        res = [h for (h,) in self.db.query(q)]
        self.assertEqual(len(res), 3)

    def test_faddr_table_returns_qualified_table(self):
        addr = self.db.faddr_table(self.feat)
        parts = addr.split(".")
        self.assertEqual(len(parts), 2)
        self.assertEqual(parts[0], self.name)
        self.assertEqual(parts[1], self.feat)

    def test_multiple_values_per_hash(self):
        # Each hash in the fixture has both val1 and val2; both queries return all hashes
        res1 = self.query(self.feat, self.val1)
        res2 = self.query(self.feat, self.val2)
        self.assertSetEqual(set(res1), {"a", "b", "c"})
        self.assertSetEqual(set(res2), {"a", "b", "c"})

    def test_collapse_none_returns_multiple_rows_for_multivalued_hash(self):
        # Each hash has val1 and val2; collapse=None should give 2 rows per hash
        q = GBDQuery(self.db, "").build_query(resolve=[self.feat], collapse=None)
        rows = self.db.query(q)
        hash_a_rows = [r for r in rows if r[0] == "a"]
        self.assertEqual(len(hash_a_rows), 2)
        values = {r[1] for r in hash_a_rows}
        self.assertSetEqual(values, {self.val1, self.val2})

    def test_delete_one_value_leaves_other(self):
        self.db.delete(self.feat, [self.val1], ["a"])
        # val2 still there for "a"
        res = self.query(self.feat, self.val2)
        self.assertIn("a", res)
        # val1 gone for "a", still present for b and c
        res = self.query(self.feat, self.val1)
        self.assertNotIn("a", res)
        self.assertSetEqual(set(res), {"b", "c"})

    def test_neq_excludes_hashes_with_the_value(self):
        # For 1:n: feat != val1 uses NOT IN → returns hashes that have NO val1 entry.
        # Delete val1 from "b" and "c" so only "a" retains val1.
        self.db.delete(self.feat, [self.val1], ["b"])
        self.db.delete(self.feat, [self.val1], ["c"])
        # a: val1 + val2;  b: val2 only;  c: val2 only
        # feat != val1 → hashes with NO val1 → {b, c}
        qb = GBDQuery(self.db, "{} != {}".format(self.feat, self.val1))
        q = qb.build_query()
        res = [h for (h,) in self.db.query(q)]
        self.assertSetEqual(set(res), {"b", "c"})
        self.assertNotIn("a", res)