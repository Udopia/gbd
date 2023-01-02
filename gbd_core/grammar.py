
# MIT License

# Copyright (c) 2023 Markus Iser, Karlsruhe Institute of Technology (KIT)

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

from gbd_core.database import DatabaseException

class ParserException(Exception):
    pass

class Parser:
    GRAMMAR = r'''
        @@grammar::GBDQuery
        @@ignorecase::True

        start 
            = 
            q:query $ 
            ;

        query 
            = 
            | left:query qop:("and" | "or") ~ right:query 
            | constraint 
            | "(" q:query ")" 
            ;

        constraint 
            = 
            | col:column cop:("=" | "!=" | "<=" | ">=" | "<" | ">" ) ter:termstart
            | col:column cop:("=" | "!=") str:string 
            | col:column cop:("like" | "unlike") ~ lik:(["%"] string ["%"])
            ;

        termstart 
            = 
            t:term
            ;

        term 
            = 
            | left:(term | termend) top:("+" | "-" | "*" | "/") right:(term | termend)
            | ("(") t:(term | termend) (")")
            | constant:number
            ;

        termend
            =
            col:column
            ;

        number = /[-]?[0-9]+[.]?[0-9]*/ ;
        string = /[a-zA-Z0-9_\.\-\/\,\:]+/ ;
        column = /[a-zA-Z][a-zA-Z0-9_]*/ ;
    '''


    model = tatsu.compile(GRAMMAR)


    def __init__(self, query, verbose=False):
        try:
            self.ast = Parser.model.parse(query) if query else dict()
            if verbose:
                print("Parsed: " + query)
                print(json.dumps(tatsu.util.asjson(self.ast), indent=2))
        except tatsu.exceptions.FailedParse as e:
            raise ParserException("Failed to parse query: {}".format(str(e)))
        except tatsu.exceptions.FailedLeftRecursion as e:
            raise ParserException("Failed to parse query: {}".format(str(e)))


    def get_features(self, ast=None):
        try:
            ast = ast if ast else self.ast
            if "q" in ast:
                return self.get_features(ast["q"])
            elif "t" in ast:
                return self.get_features(ast["t"])
            elif "qop" in ast or "top" in ast:
                return self.get_features(ast["left"]) | self.get_features(ast["right"])
            elif "cop" in ast and "ter" in ast:
                return { ast["col"] } | self.get_features(ast["ter"])
            elif "col" in ast:
                return { ast["col"] }
            else: 
                return set()
        except TypeError as e:
            raise ParserException("Failed to parse query: {}".format(str(e)))


    def get_sql(self, db, ast=None):
        try:
            ast = ast if ast else self.ast
            if "q" in ast:
                return "(" + self.get_sql(db, ast["q"]) + ")"
            elif "t" in ast:
                return "(" + self.get_sql(db, ast["t"]) + ")"
            elif "qop" in ast or "top" in ast:
                operator = ast["qop"] if ast["qop"] else ast["top"]
                left = self.get_sql(db, ast["left"])
                right = self.get_sql(db, ast["right"])
                return "{} {} {}".format(left, operator, right)
            elif "cop" in ast:
                operator = "not like" if ast["cop"] == "unlike" else ast["cop"]
                feat = db.faddr_column(ast["col"])
                if "str" in ast:
                    return "{} {} '{}'".format(feat, operator, ast["str"])
                elif "lik" in ast:
                    return "{} {} {}".format(feat, operator, "".join(ast["lik"]))
                elif "ter" in ast:
                    return "{} {} {}".format(feat, operator, self.get_sql(db, ast["ter"]))
                raise ParserException("Missing right-hand side of constraint")
            elif "col" in ast:
                feature = db.faddr_column(ast["col"])
                return "CAST({} AS FLOAT)".format(feature)
            elif "constant" in ast:
                return ast["constant"]
            else:
                return "1=1"
        except TypeError as e:
            raise ParserException("Failed to parse query: {}".format(str(e)))
        except DatabaseException as e:
            raise ParserException("Failed to parse query: {}".format(str(e)))

