
import os
import unittest

from gbd_core.api import GBD, GBDException
from gbd_core.schema import Schema

from tests import util

class APITestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.file1 = util.get_random_unique_filename('test1', '.db')
        self.file2 = util.get_random_unique_filename('test2', '.db')
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