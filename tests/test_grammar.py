
import unittest
import sqlite3
import os

from gbd_core.grammar import Parser, ParserException
from gbd_core.database import Database
from gbd_core.schema import Schema
import tests.util as util


class ParserFeatureExtractionTest(unittest.TestCase):
    """Tests for Parser.get_features() - collecting column names from a query AST."""

    def setUp(self) -> None:
        return super().setUp()

    def tearDown(self) -> None:
        return super().tearDown()

    def test_empty_query_returns_empty_set(self):
        self.assertEqual(Parser(None).get_features(), set())
        self.assertEqual(Parser("").get_features(), set())

    def test_not_operator_extracts_feature(self):
        self.assertEqual(Parser("not a = 1").get_features(), {"a"})
        self.assertEqual(Parser("not (a = 1 and b = 2)").get_features(), {"a", "b"})

    def test_query_nesting(self):
        parser = Parser("a = 1")
        self.assertEqual(parser.get_features(), set(["a"]))
        parser = Parser("a = 1 and b = 2")
        self.assertEqual(parser.get_features(), set(["a", "b"]))
        parser = Parser("a = 1 and (b = 2 or c = 3)")
        self.assertEqual(parser.get_features(), set(["a", "b", "c"]))
        parser = Parser("(b = 2 or c = 3) and a = 1")
        self.assertEqual(parser.get_features(), set(["a", "b", "c"]))

    def test_query_string_constraints(self):
        parser = Parser("a = val1")
        self.assertEqual(parser.get_features(), set(["a"]))
        parser = Parser("a = val1 and b != val2")
        self.assertEqual(parser.get_features(), set(["a", "b"]))
        parser = Parser("a like val1")
        self.assertEqual(parser.get_features(), set(["a"]))
        parser = Parser("a like val%")
        self.assertEqual(parser.get_features(), set(["a"]))
        parser = Parser("a like %val")
        self.assertEqual(parser.get_features(), set(["a"]))
        parser = Parser("a like %val%")
        self.assertEqual(parser.get_features(), set(["a"]))
        parser = Parser("a like val% and b unlike val%")
        self.assertEqual(parser.get_features(), set(["a", "b"]))        
        with self.assertRaises(ParserException):
            parser = Parser("a = %val%")

    def test_query_arithmetic_constraints(self):
        parser = Parser("a = (1 + 2)")
        self.assertEqual(parser.get_features(), set(["a"]))
        parser = Parser("a = (1 - 2)")
        self.assertEqual(parser.get_features(), set(["a"]))
        parser = Parser("a = ((1 + 2) / b)")
        self.assertEqual(parser.get_features(), set(["a", "b"]))
        parser = Parser("a = (b)")
        self.assertEqual(parser.get_features(), set(["a", "b"]))
        parser = Parser("a = b")
        self.assertEqual(parser.get_features(), set(["a"]))

    def test_bare_string_rhs_is_not_a_feature(self):
        # "a = b" - b on the right is a string literal (no parentheses), not a feature
        self.assertEqual(Parser("a = b").get_features(), {"a"})

    def test_explicit_context(self):
        parser = Parser("c:a = 1")
        self.assertEqual(parser.get_features(), set(["c:a"]))
        parser = Parser("c:a = 1 and d:b = 2")
        self.assertEqual(parser.get_features(), set(["c:a", "d:b"]))


class ParserSyntaxErrorTest(unittest.TestCase):
    """Tests that ill-formed queries raise ParserException."""

    def test_wildcard_in_equality_raises(self):
        with self.assertRaises(ParserException):
            Parser("a = %val%")

    def test_missing_rhs_raises(self):
        with self.assertRaises(ParserException):
            Parser("a =")

    def test_unclosed_paren_raises(self):
        with self.assertRaises(ParserException):
            Parser("(a = 1")

    def test_bare_value_no_operator_raises(self):
        with self.assertRaises(ParserException):
            Parser("value1")

    def test_double_operator_raises(self):
        with self.assertRaises(ParserException):
            Parser("a = 1 and and b = 2")

    def test_like_without_value_raises(self):
        with self.assertRaises(ParserException):
            Parser("a like")


class ParserGetSqlTest(unittest.TestCase):
    """Tests for Parser.get_sql() - verifies the emitted SQL fragment is structurally
    correct for both 1:1 and 1:n features and for all boolean operators.

    Uses a real single-file Database so that feature cardinality (1:1 vs 1:n) is
    respected by get_sql.
    """

    def setUp(self):
        self.file = util.get_random_unique_filename("test_grammar", ".db")
        sqlite3.connect(self.file).close()
        self.dbname = Schema.dbname_from_path(self.file)
        self.db = Database([self.file])
        # ufeat: 1:1 feature (column in features table, default="empty")
        self.db.create_feature("ufeat", default_value="empty")
        # mfeat: 1:n feature (separate mfeat(hash, value) table)
        self.db.create_feature("mfeat", default_value=None)

    def tearDown(self):
        if os.path.exists(self.file):
            os.remove(self.file)

    def sql(self, query_str):
        return Parser(query_str).get_sql(self.db)

    # ---- 1:1 feature: comparisons are emitted inline (no subquery) ----

    def test_1to1_eq_string_is_inline(self):
        s = self.sql("ufeat = foo")
        self.assertIn("'foo'", s)
        self.assertNotIn("SELECT", s)

    def test_1to1_neq_string_is_inline(self):
        s = self.sql("ufeat != foo")
        self.assertIn("!=", s)
        self.assertNotIn("SELECT", s)

    def test_1to1_numeric_uses_cast_inline(self):
        s = self.sql("ufeat > 5")
        self.assertIn("CAST", s)
        self.assertIn("> 5", s)
        self.assertNotIn("SELECT", s)

    def test_1to1_like_is_inline(self):
        s = self.sql("ufeat like foo%")
        self.assertIn("like", s.lower())
        self.assertIn("foo%", s)
        self.assertNotIn("SELECT", s)

    def test_1to1_unlike_is_inline_not_like(self):
        s = self.sql("ufeat unlike foo%")
        self.assertIn("not like", s.lower())
        self.assertNotIn("SELECT", s)

    # ---- 1:n feature: equality / like wrapped in subquery ----------------

    def test_1ton_eq_string_uses_in_subquery(self):
        s = self.sql("mfeat = foo")
        self.assertIn("IN", s.upper())
        self.assertIn("SELECT", s)
        self.assertIn("'foo'", s)

    def test_1ton_neq_string_uses_not_in_subquery(self):
        s = self.sql("mfeat != foo")
        self.assertIn("NOT IN", s.upper())
        self.assertIn("SELECT", s)

    def test_1ton_like_uses_in_subquery(self):
        s = self.sql("mfeat like foo%")
        self.assertIn("IN", s.upper())
        self.assertIn("SELECT", s)
        self.assertIn("like", s.lower())

    def test_1ton_unlike_uses_not_in_subquery(self):
        s = self.sql("mfeat unlike foo%")
        self.assertIn("NOT IN", s.upper())
        self.assertIn("SELECT", s)

    def test_1ton_numeric_gt_uses_in_subquery(self):
        s = self.sql("mfeat > 5")
        self.assertIn("IN", s.upper())
        self.assertIn("SELECT", s)
        self.assertIn("> 5", s)

    def test_1ton_numeric_neq_uses_in_subquery_with_neq_inside(self):
        # For 1:n numeric, ALL operators including != are wrapped in IN(SELECT ... WHERE cast(v) op n)
        # This is different from the string != case which uses NOT IN.
        s = self.sql("mfeat != 5")
        self.assertIn("IN", s.upper())
        self.assertIn("SELECT", s)
        self.assertIn("!= 5", s)

    # ---- boolean operators -----------------------------------------------

    def test_and_emits_sql_and(self):
        s = self.sql("ufeat = foo and ufeat = bar")
        self.assertIn("AND", s.upper())

    def test_or_emits_sql_or(self):
        s = self.sql("ufeat = foo or ufeat = bar")
        self.assertIn("OR", s.upper())

    def test_not_emits_sql_not(self):
        s = self.sql("not ufeat = foo")
        # The top-level 'q' wrapper adds outer parens; NOT appears inside
        self.assertIn("NOT", s.upper())

    def test_empty_query_returns_always_true(self):
        self.assertEqual(Parser(None).get_sql(self.db), "1=1")
        self.assertEqual(Parser("").get_sql(self.db), "1=1")