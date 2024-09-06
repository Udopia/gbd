import unittest
import sqlite3
import os

from gbd_core.schema import Schema
from gbd_core.database import Database
from gbd_core.query import GBDQuery

import tests.util as util

class QueryNonUniqueTestCase(unittest.TestCase):

    feat = "nonuniquefeature"
    feat2 = "nonuniquefeature2"
    feat3 = "numericfeature"
    val1 = "value1"
    val2 = "value2"
    hashes = [ "a", "b", "c" ]

    def setUp(self) -> None:
        self.file1 = util.get_random_unique_filename('test1', '.db')
        self.file2 = util.get_random_unique_filename('test2', '.db')
        sqlite3.connect(self.file1).close()
        sqlite3.connect(self.file2).close()
        self.dbname1 = Schema.dbname_from_path(self.file1)
        self.dbname2 = Schema.dbname_from_path(self.file2)
        self.db = Database([self.file1,self.file2], verbose=False)

        self.db.create_feature(self.feat, default_value=None, target_db=self.dbname1)
        self.db.set_values(self.feat, self.val1, self.hashes)

        self.db.create_feature(self.feat, default_value=None, target_db=self.dbname2)
        self.db.set_values(self.feat, self.val1, self.hashes[:1], target_db=self.dbname2)
        self.db.set_values(self.feat, self.val2, self.hashes, target_db=self.dbname2)

        self.db.create_feature(self.feat2, default_value=None, target_db=self.dbname2)
        self.db.set_values(self.feat2, self.val2, self.hashes)

        self.db.create_feature(self.feat3, default_value=0, target_db=self.dbname1)
        self.db.set_values(self.feat3, 1, self.hashes[0])
        self.db.set_values(self.feat3, 10, self.hashes[1])
        self.db.set_values(self.feat3, 100, self.hashes[2])

        return super().setUp()

    def tearDown(self) -> None:
        if os.path.exists(self.file1):
            os.remove(self.file1)
        if os.path.exists(self.file2):
            os.remove(self.file2)
        return super().tearDown()
    
    def simple_query(self, feat, val, dbname=None):
        if dbname is None:
            return self.query("{}={}".format(feat, val))
        else:
            return self.query("{}:{}={}".format(dbname, feat, val))
    
    def query(self, query):
        q = GBDQuery(self.db, query).build_query()
        return [ hash for (hash, ) in self.db.query(q) ]

    def dump(self):
        import sqlite3
        conn = sqlite3.connect(self.file)
        for line in conn.iterdump():
            print(line)
        conn.close()


    def test_feature_precedence_rules(self):
        res = self.simple_query(self.feat, self.val1)
        self.assertEqual(len(res), 3)
        res = self.simple_query(self.feat, self.val2)
        self.assertEqual(len(res), 0)
        res = self.simple_query(self.feat, self.val1, self.dbname1)
        self.assertEqual(len(res), 3)
        res = self.simple_query(self.feat, self.val2, self.dbname2)
        self.assertEqual(len(res), 3)

    def test_string_inequality(self):
        res = self.query("{} < {}".format(self.feat, self.val2))
        self.assertEqual(len(res), 3)
        res = self.query("{} > {}".format(self.feat, self.val1))
        self.assertEqual(len(res), 0)

    def test_numeric_inequality(self):
        res = self.query("{} < 2".format(self.feat3))
        self.assertEqual(len(res), 1)
        
    def test_multivalued_subselect(self):
        res = self.query("{db}:{f} != {v1} and {db}:{f} = {v2}".format(f=self.feat, v1=self.val1, v2=self.val2, db=self.dbname2))
        self.assertEqual(len(res), 2)

    def test_feature_accessible(self):
        res = self.simple_query(self.feat2, self.val2)
        self.assertEqual(len(res), 3)