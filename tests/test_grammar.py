
import unittest

from gbd_core.grammar import Parser, ParserException

class SchemaTestCase(unittest.TestCase):

    def setUp(self) -> None:
        return super().setUp()

    def tearDown(self) -> None:
        return super().tearDown()

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

    def test_explicit_context(self):
        parser = Parser("c:a = 1")
        self.assertEqual(parser.get_features(), set(["c:a"]))
        parser = Parser("c:a = 1 and d:b = 2")
        self.assertEqual(parser.get_features(), set(["c:a", "d:b"]))