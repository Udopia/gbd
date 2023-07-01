##!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import unittest
import sqlite3

from gbd_core.database import Database
from gbd_core.schema import Schema

from tests import util

class SchemaTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.file = util.get_random_unique_filename('test', '.db')
        sqlite3.connect(self.file).close()
        self.name = Schema.dbname_from_path(self.file)
        self.db = Database([self.file], verbose=False)
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
        finfo = self.db.find(FEAT)
        self.assertEqual(finfo.table, "features")
        self.assertEqual(finfo.column, FEAT)
        self.assertEqual(finfo.default, "empty")
        self.assertEqual(finfo.database, self.name)
        
    def test_create_nonunique_feature(self):
        FEAT = "featB"
        self.db.create_feature(FEAT, default_value=None)
        self.assertIn(FEAT, self.db.get_features())
        self.assertIn("features", self.db.get_tables())
        finfo = self.db.find(FEAT)
        self.assertEqual(finfo.table, FEAT)
        self.assertEqual(finfo.column, "value")
        self.assertEqual(finfo.default, None)
        self.assertEqual(finfo.database, self.name)

