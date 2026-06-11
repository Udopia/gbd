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


class SchemaUtilityTest(unittest.TestCase):
    """Tests for Schema class-level utilities that don't require a full Database."""

    def test_dbname_from_path_strips_extension(self):
        self.assertEqual(Schema.dbname_from_path("/path/to/cnf_sc2021.db"), "cnf_sc2021")

    def test_dbname_from_path_sanitizes_hyphens(self):
        self.assertEqual(Schema.dbname_from_path("/path/to/my-db.db"), "my_db")

    def test_dbname_from_path_sanitizes_dots(self):
        name = Schema.dbname_from_path("/path/to/my.bench.db")
        self.assertTrue(name.isidentifier())

    def test_dbname_from_path_digit_start_gets_prefix(self):
        from gbd_core import contexts
        name = Schema.dbname_from_path("/data/2021data.db")
        self.assertFalse(name[0].isdigit(), "dbname must not start with a digit")
        self.assertIn("2021data", name)

    def test_context_from_name_known_prefix(self):
        self.assertEqual(Schema.context_from_name("cnf_sc2021"), "cnf")
        self.assertEqual(Schema.context_from_name("kis_benchmark"), "kis")
        self.assertEqual(Schema.context_from_name("wcnf_maxsat"), "wcnf")
        self.assertEqual(Schema.context_from_name("sancnf_2022"), "sancnf")

    def test_context_from_name_unknown_falls_back_to_default(self):
        from gbd_core import contexts
        default = contexts.default_context()
        self.assertEqual(Schema.context_from_name("testdb"), default)
        self.assertEqual(Schema.context_from_name("unknown_prefix"), default)

    def test_valid_feature_reserved_names_raise(self):
        from gbd_core.schema import SchemaException
        for name in ("hash", "value", "features"):
            with self.assertRaises(SchemaException, msg=f"'{name}' should be rejected"):
                Schema.valid_feature_or_raise(name)

    def test_valid_feature_invalid_chars_raise(self):
        from gbd_core.schema import SchemaException
        with self.assertRaises(SchemaException):
            Schema.valid_feature_or_raise("123abc")   # starts with digit
        with self.assertRaises(SchemaException):
            Schema.valid_feature_or_raise("feat-name")  # hyphen not allowed

    def test_valid_feature_accepts_valid_names(self):
        # Must not raise
        Schema.valid_feature_or_raise("my_feature")
        Schema.valid_feature_or_raise("featA1")
        Schema.valid_feature_or_raise("a")


class FeatureCreationTest(unittest.TestCase):
    """Tests for create_feature focusing on 1:1 vs 1:n structural differences."""

    def setUp(self):
        self.file = util.get_random_unique_filename("test_schema2", ".db")
        sqlite3.connect(self.file).close()
        self.name = Schema.dbname_from_path(self.file)
        self.db = Database([self.file])

    def tearDown(self):
        if os.path.exists(self.file):
            os.remove(self.file)

    def test_1to1_feature_stored_in_features_table(self):
        self.db.create_feature("ufeat", default_value="empty")
        finfo = self.db.find("ufeat")
        self.assertEqual(finfo.table, "features")
        self.assertEqual(finfo.column, "ufeat")
        self.assertEqual(finfo.default, "empty")

    def test_1ton_feature_stored_in_separate_table(self):
        self.db.create_feature("mfeat", default_value=None)
        finfo = self.db.find("mfeat")
        self.assertEqual(finfo.table, "mfeat")
        self.assertEqual(finfo.column, "value")
        self.assertIsNone(finfo.default)

    def test_hash_feature_created_alongside_first_feature(self):
        self.db.create_feature("myfeat", default_value="empty")
        finfo = self.db.find("hash")
        self.assertEqual(finfo.table, "features")
        self.assertEqual(finfo.column, "hash")

    def test_create_feature_duplicate_raises(self):
        from gbd_core.schema import SchemaException
        self.db.create_feature("myfeat", default_value="empty")
        with self.assertRaises(SchemaException):
            self.db.create_feature("myfeat", default_value="empty")

    def test_create_feature_permissive_allows_duplicate(self):
        self.db.create_feature("myfeat", default_value="empty")
        # Should not raise
        self.db.create_feature("myfeat", default_value="empty", permissive=True)

    def test_context_inferred_from_filename_prefix(self):
        from gbd_core import contexts
        context = self.db.dcontext(self.name)
        # Test file has no context prefix, falls back to default
        self.assertEqual(context, contexts.default_context())

    def test_context_inferred_for_cnf_prefixed_db(self):
        import tempfile
        # Create a temp file whose name starts with "cnf_"
        tmpdir = os.path.dirname(self.file)
        cnf_path = os.path.join(tmpdir, "cnf_testbench.db")
        sqlite3.connect(cnf_path).close()
        try:
            db2 = Database([cnf_path])
            db2name = Schema.dbname_from_path(cnf_path)
            self.assertEqual(db2.dcontext(db2name), "cnf")
        finally:
            if os.path.exists(cnf_path):
                os.remove(cnf_path)

