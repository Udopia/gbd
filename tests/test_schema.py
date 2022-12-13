##!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import unittest

from gbd.db import Database
from gbd.schema import Schema

class SchemaTestCase(unittest.TestCase):

    file = "test.db"
    name = Schema.dbname_from_path(file)

    def setUp(self) -> None:
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

