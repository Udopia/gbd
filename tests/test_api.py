
import os
import unittest
import sqlite3

from gbd_core.api import GBD, GBDException
from gbd_core.schema import Schema

from tests import util

class APITestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.file1 = util.get_random_unique_filename('test1', '.db')
        self.file2 = util.get_random_unique_filename('test2', '.db')
        sqlite3.connect(self.file1).close()
        sqlite3.connect(self.file2).close()
        self.name1 = Schema.dbname_from_path(self.file1)
        self.name2 = Schema.dbname_from_path(self.file2)
        self.api = GBD([self.file1, self.file2])
        return super().setUp()

    def tearDown(self) -> None:
        if os.path.exists(self.file1):
            os.remove(self.file1)
        if os.path.exists(self.file2):
            os.remove(self.file2)
        return super().tearDown()

    def test_databases_exist(self):
        self.assertEquals(self.api.get_databases(), [ self.name1, self.name2 ])
        self.assertEquals(self.api.get_database_path(self.name1), self.file1)
        self.assertEquals(self.api.get_database_path(self.name2), self.file2)

    def test_create_feature(self):
        self.api.create_feature("A", None, self.name1)
        self.assertTrue(self.api.feature_exists("A"))
        self.api.create_feature("A", None, self.name2)
        api2 = GBD([self.file2])
        self.assertTrue(api2.feature_exists("A"))
        with self.assertRaises(GBDException):
            self.api.create_feature("A", None, self.name1)
        with self.assertRaises(GBDException):
            self.api.create_feature("A", None, self.name2)

    def test_delete_feature(self):
        self.api.create_feature("A", None, self.name1)
        self.api.create_feature("A", None, self.name2)
        self.api.delete_feature("A", self.name1)
        self.assertFalse(self.api.feature_exists("A", self.name1))
        self.assertTrue(self.api.feature_exists("A"))
        self.assertTrue(self.api.feature_exists("A", self.name2))
        self.api.delete_feature("A")
        self.assertFalse(self.api.feature_exists("A"))

    def test_rename_feature(self):
        self.api.create_feature("A", None, self.name1)
        self.api.create_feature("B", None, self.name1)
        self.api.create_feature("A", None, self.name2)
        self.api.rename_feature("A", "B", self.name2)
        self.assertFalse(self.api.feature_exists("A", self.name2))
        self.assertTrue(self.api.feature_exists("B", self.name2))
        self.assertTrue(self.api.feature_exists("A", self.name1))
        self.assertTrue(self.api.feature_exists("B", self.name1))
        with self.assertRaises(GBDException):
            self.api.rename_feature("A", "B", self.name1)

    def test_set_values(self):
        self.api.create_feature("A", None, self.name1) # feature is multi-valued
        self.api.create_feature("B", "empty", self.name1) # feature has default value
        self.api.create_feature("A", "empty", self.name2) # shadowed feature
        # value1 (set values, default values emerge)
        self.api.set_values("A", "value1", [ str(i) for i in range(100) ], self.name1)
        df = self.api.query("A = value1", resolve=["A", "B"])
        self.assertCountEqual(df['hash'].tolist(), [ str(i) for i in range(100) ])
        self.assertCountEqual(df['A'].tolist(), [ "value1" for _ in range(100) ])
        self.assertCountEqual(df['B'].tolist(), [ "empty" for _ in range(100) ])
        # value2 (set values, feature is multi-valued)
        self.api.set_values("A", "value2", [ str(i) for i in range(50) ], self.name1)
        df = self.api.query("A = value1 or A = value2", resolve=["A"], collapse=None)
        self.assertCountEqual(df['A'].tolist(), [ "value2" for _ in range(50) ] + [ "value1" for _ in range(100) ])
        # value3 (set values of shadowed feature by specifying target-database)
        self.api.set_values("A", "value3", [ str(i) for i in range(50) ], self.name2)
        df = self.api.query("A = value1 or A = value2", resolve=["A"], collapse=None)
        self.assertCountEqual(df['A'].tolist(), [ "value2" for _ in range(50) ] + [ "value1" for _ in range(100) ])
        self.api.database.commit()
        api2 = GBD([self.file2])
        df = api2.query("A = value3", resolve=["A"])
        self.assertCountEqual(df["A"].tolist(), [ "value3" for _ in range(50) ])

    def test_reset_values(self):
        self.api.create_feature("A", None, self.name1)
        self.api.create_feature("B", "empty", self.name1)
        self.api.create_feature("A", "empty", self.name2)
        self.api.set_values("A", "value1", [ str(i) for i in range(100) ], self.name1)
        self.api.set_values("A", "value2", [ str(i) for i in range(100) ], self.name1)
        self.api.set_values("B", "value3", [ str(i) for i in range(100) ], self.name1)
        self.api.set_values("A", "value1", [ str(i) for i in range(100) ], self.name2)
        # reset values in A
        self.api.reset_values("A", [ "value1" ], [ str(i) for i in range(50) ], self.name1)
        df = self.api.query(None, hashes=[ str(i) for i in range(100) ], resolve=["A"], collapse=None)
        self.assertCountEqual(df['A'].tolist(), [ "value1" for _ in range(50) ] + [ "value2" for _ in range(100) ])
        # reset values in B
        self.api.reset_values("B", [ "value3" ], [ str(i) for i in range(50) ], self.name1)
        df = self.api.query(None, hashes=[ str(i) for i in range(100) ], resolve=["B"])
        self.assertCountEqual(df['B'].tolist(), [ "value3" for _ in range(50) ] + [ "empty" for _ in range(50) ])
        # reset values in shadowed A
        self.api.database.verbose = True
        self.api.reset_values("A", [ "value1" ], [ str(i) for i in range(50) ], self.name2)
        self.api.database.commit()
        api2 = GBD([self.file2])
        df = api2.query("A = value1", resolve=["A"])
        self.assertCountEqual(df["A"].tolist(), [ "value1" for _ in range(50) ])