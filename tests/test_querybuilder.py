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
        self.db.set_values(self.feat, self.val2, self.hashes, target_db=self.dbname2)

        self.db.create_feature(self.feat2, default_value=None, target_db=self.dbname2)
        self.db.set_values(self.feat2, self.val2, self.hashes)

        return super().setUp()

    def tearDown(self) -> None:
        if os.path.exists(self.file1):
            os.remove(self.file1)
        if os.path.exists(self.file2):
            os.remove(self.file2)
        return super().tearDown()
    
    def query(self, feat, val, dbname=None):
        if dbname is None:
            qb = GBDQuery(self.db, "{}={}".format(feat, val))
        else:
            qb = GBDQuery(self.db, "{}:{}={}".format(dbname, feat, val))
        q = qb.build_query()
        return [ hash for (hash, ) in self.db.query(q) ]

    def dump(self):
        import sqlite3
        conn = sqlite3.connect(self.file)
        for line in conn.iterdump():
            print(line)
        conn.close()


    def test_feature_precedence_rules(self):
        res = self.query(self.feat, self.val1)
        self.assertEqual(len(res), 3)
        res = self.query(self.feat, self.val2)
        self.assertEqual(len(res), 0)
        res = self.query(self.feat, self.val1, self.dbname1)
        self.assertEqual(len(res), 3)
        res = self.query(self.feat, self.val2, self.dbname2)
        self.assertEqual(len(res), 3)

    def test_feature_accessible(self):
        res = self.query(self.feat2, self.val2)
        self.assertEqual(len(res), 3)