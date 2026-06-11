# MIT License

# Copyright (c) 2025 Ashlin Iser, Karlsruhe Institute of Technology (KIT)

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

import tatsu
import json

from gbd_core.database import Database, DatabaseException


class ParserException(Exception):
    """Raised when a GBD query string cannot be parsed or when a referenced
    feature cannot be resolved in the database."""
    pass


class Parser:
    """Parses GBD query strings and compiles them to SQL WHERE fragments.

    The GBD query language is a small filter DSL:

    - Boolean logic: ``and``, ``or``, ``not``
    - Comparisons: ``=``, ``!=``, ``<``, ``>``, ``<=``, ``>=``
    - Pattern matching: ``like`` / ``unlike`` with optional leading/trailing ``%`` wildcard
    - Feature references: ``feature``, ``context:feature``, or ``database:feature``
    - Right-hand side: unquoted or single/double-quoted strings, integers/floats, or
      parenthesised arithmetic terms (``+``, ``-``, ``*``, ``/``)

    **1:1 vs 1:n feature translation** (see :py:meth:`get_sql`):

    * *1:1 features* (``FeatureInfo.default != None``) are stored as columns of the central
      ``features`` table; comparisons are emitted inline.
    * *1:n features* (``FeatureInfo.default is None``) are stored in a separate
      ``{name}(hash, value)`` table; equality / inequality / like comparisons are wrapped in
      a subquery so they act as set-membership tests (``IN`` / ``NOT IN``).
      Numeric inequality and arithmetic-term comparisons on 1:n features fall through to an
      inline comparison, meaning "at least one value satisfies the condition".
      See ``Issues.md`` #2 and #3.

    String values are interpolated directly into the SQL string without escaping.
    See ``Issues.md`` #1; SQL injection risk.
    """
    GRAMMAR = r"""
        @@grammar::GBDQuery
        @@ignorecase::True

        start 
            = 
            q:query $ 
            ;

        query 
            = 
            | left:query qop:("and" | "or") ~ right:query 
            | qop:("not") ~ q:query
            | constraint 
            | "(" q:query ")" 
            ;

        constraint 
            = 
            | col:(dbname ":" column | column) cop:("=" | "!=" | "<=" | ">=" | "<" | ">" ) ter:termstart
            | col:(dbname ":" column | column) cop:("=" | "!=" | "<=" | ">=" | "<" | ">" ) num:number 
            | col:(dbname ":" column | column) cop:("=" | "!=" | "<=" | ">=" | "<" | ">" ) str:string 
            | col:(dbname ":" column | column) cop:("like" | "unlike") ~ pre:["%"] lik:string suf:["%"]
            ;

        termstart 
            = 
            ("(") t:term (")")
            ;

        term 
            = 
            | left:term top:("+" | "-" | "*" | "/") right:term
            | ("(") t:term (")")
            | constant:number
            | col:(dbname ":" column | column)
            ;

        string
            =
            | "'" @:singlequotedstring "'"
            | '"' @:doublequotedstring '"'
            | /[a-zA-Z0-9_\.\-\/\,\:\+\=\@]+/
            ;

        # number = /[-]?[0-9]+[.]?[0-9]*/ ;
        number = /[-]?[0-9]+(?:\.[0-9]+)?(?![A-Za-z0-9_])/ ;
        singlequotedstring = /[a-zA-Z0-9_\.\-\/\,\:\+\=\@\s"\*\\]+/ ;
        doublequotedstring = /[a-zA-Z0-9_\.\-\/\,\:\+\=\@\s'\*\\]+/ ;
        column = /[a-zA-Z][a-zA-Z0-9_]*/ ;
        dbname = /[a-zA-Z][a-zA-Z0-9_]*/ ;
    """

    model = tatsu.compile(GRAMMAR)

    def __init__(self, query, verbose=False):
        """Parse *query* into an internal AST.

        Args:
            query (str | None): GBD query string, e.g. ``"filename like foo% and vars > 100"``.
                Pass ``None`` or an empty string for an unconditional (match-all) query.
            verbose (bool): If ``True``, print the raw query and its JSON AST to stdout.

        Raises:
            ParserException: If *query* is syntactically invalid.
        """
        try:
            self.ast = Parser.model.parse(query) if query else dict()
            if verbose:
                print("Parsed: " + query)
                print(json.dumps(tatsu.util.asjson(self.ast), indent=2))
        except tatsu.exceptions.FailedParse as e:
            raise ParserException(f"Failed to parse query: {str(e)}") from e

    def get_features(self, ast=None):
        """Return the set of feature names referenced anywhere in the query.

        Walks the AST recursively and collects every ``col`` leaf. The returned names
        are used by :py:class:`GBDQuery` to verify that all referenced
        features exist before executing a query.

        Args:
            ast (dict | None): Sub-tree to walk; defaults to the root AST.

        Returns:
            set[str]: Feature names, possibly qualified as ``"context:feature"`` or
            ``"database:feature"`` if the query uses such prefixes.

        Raises:
            ParserException: On unexpected AST structure.
        """
        # import pprint
        # pp = pprint.PrettyPrinter(depth=6)
        # pp.pprint(ast)
        try:
            ast = ast if ast else self.ast
            if "q" in ast:
                return self.get_features(ast["q"])
            elif "t" in ast:
                return self.get_features(ast["t"])
            elif "qop" in ast or "top" in ast:
                return self.get_features(ast["left"]) | self.get_features(ast["right"])
            elif "cop" in ast and "ter" in ast:
                return {"".join(ast["col"])} | self.get_features(ast["ter"])
            elif "col" in ast:
                return {"".join(ast["col"])}
            else:
                return set()
        except TypeError as e:
            raise ParserException(f"Failed to parse query: {str(e)}") from e

    def get_sql(self, db: Database, ast=None):
        """Recursively compile the parsed AST into a SQL WHERE fragment.

        Column addresses are fully qualified as ``database.table.column`` via
        :py:meth:`Database.faddr`. The translation is cardinality-aware:

        * **1:1, string** ``col = 'v'`` -> ``db.features.col = 'v'``
        * **1:n, string** ``col = 'v'`` ->
          ``db.col.hash IN (SELECT db.col.hash FROM db.col WHERE db.col.value = 'v')``
        * **1:n, string** ``col != 'v'`` ->
          ``db.col.hash NOT IN (SELECT … WHERE db.col.value = 'v')``
        * **1:n, like** ``col like foo%`` ->
          ``db.col.hash IN (SELECT … WHERE db.col.value like 'foo%')``
        * **1:n, numeric** ``col > 5`` ->
          ``CAST(db.col.value AS FLOAT) > 5``  *(any-row semantics - see Issues.md #2)*
        * **1:n, term** ``col != (expr)`` ->
          ``db.col.hash NOT IN (SELECT … WHERE CAST(db.col.value AS FLOAT) = expr)``
        * **1:n, term** ``col op (expr)`` (other ops) -> 
          ``CAST(db.col.value AS FLOAT) op expr``  *(any-row semantics - see Issues.md #3)*

        Args:
            db (Database): Used to resolve feature addresses and determine cardinality.
            ast (dict | None): Sub-tree to compile; defaults to the root AST.

        Returns:
            str: SQL expression fragment suitable for embedding in a WHERE clause.

        Raises:
            ParserException: If the AST is malformed or a feature cannot be resolved.
        """
        try:
            ast = ast if ast else self.ast
            if "qop" in ast and ast["qop"] == "not":
                return "NOT (" + self.get_sql(db, ast["q"]) + ")"
            if "q" in ast:
                return "(" + self.get_sql(db, ast["q"]) + ")"
            if "t" in ast:
                return "(" + self.get_sql(db, ast["t"]) + ")"
            if "qop" in ast or "top" in ast:  # query operator or term operator
                operator = ast["qop"] if ast["qop"] else ast["top"]
                left = self.get_sql(db, ast["left"])
                right = self.get_sql(db, ast["right"])
                return f"{left} {operator} {right}"
            if "cop" in ast:  # constraint operator
                operator = "not like" if ast["cop"] == "unlike" else ast["cop"]
                feat = db.faddr("".join(ast["col"]))
                feat_is_1_n = db.find("".join(ast["col"])).default is None
                if "str" in ast:  # cop:("=" | "!=")
                    if feat_is_1_n:
                        table = db.faddr_table("".join(ast["col"]))
                        setop = "IN" if ast["cop"] == "=" else "NOT IN"
                        return "{t}.hash {o} (SELECT {t}.hash FROM {t} WHERE {f} = '{s}')".format(o=setop, t=table, f=feat, s=ast["str"])
                    return f"{feat} {operator} '{ast['str']}'"
                if "num" in ast:  # cop:("=" | "!=" | "<=" | ">=" | "<" | ">" )
                    if feat_is_1_n:
                        table = db.faddr_table("".join(ast["col"]))
                        return "{t}.hash IN (SELECT {t}.hash FROM {t} WHERE CAST({f} AS FLOAT) {o} {s})".format(o=operator, t=table, f=feat, s=ast["num"])
                    return f"CAST({feat} AS FLOAT) {operator} {ast['num']}"
                if "lik" in ast:  # cop:("like" | "unlike")
                    if feat_is_1_n:
                        table = db.faddr_table("".join(ast["col"]))
                        setop = "IN" if ast["cop"] == "like" else "NOT IN"
                        s = (ast.get("pre") or "") + ast["lik"] + (ast.get("suf") or "")
                        return "{t}.hash {o} (SELECT {t}.hash FROM {t} WHERE {f} like '{s}')".format(
                            o=setop, t=table, f=feat, s=s
                        )
                    s = (ast.get("pre") or "") + ast["lik"] + (ast.get("suf") or "")
                    return f"{feat} {operator} '{s}'"
                if "ter" in ast:  # cop:("=" | "!=" | "<=" | ">=" | "<" | ">" )
                    if feat_is_1_n and ast["cop"] == "!=":
                        table = db.faddr_table("".join(ast["col"]))
                        setop = "NOT IN" if ast["cop"] == "!=" else "IN"
                        cop = "=" if ast["cop"] == "!=" else ast["cop"]
                        return "{t}.hash {o} (SELECT {t}.hash FROM {t} WHERE CAST({f} AS FLOAT) {c} {s})".format(
                            o=setop, c=cop, t=table, f=feat, s=self.get_sql(db, ast["ter"])
                        )
                    return f"CAST({feat} AS FLOAT) {operator} {self.get_sql(db, ast['ter'])}"
                raise ParserException("Missing right-hand side of constraint")
            if "col" in ast:
                feature = db.faddr("".join(ast["col"]))
                return f"CAST({feature} AS FLOAT)"
            if "constant" in ast:
                return ast["constant"]
            return "1=1"
        except TypeError as e:
            raise ParserException(f"Failed to parse query: {str(e)}") from e
        except DatabaseException as e:
            raise ParserException(f"Failed to parse query: {str(e)}") from e
