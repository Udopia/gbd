
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
            | col:column cop:("=" | "!=" | "<=" | ">=" | "<" | ">" ) num:number
            | col:column cop:("like" | "unlike") ~ lik:(["%"] string ["%"])
            ;

        termstart 
            = 
            t:term
            ;

        term 
            = 
            | left:(term | termend) top:("+" | "-" | "*" | "/") ~ right:(term | termend)
            | "(" t:(term | termend) ")"
            ;

        termend
            =
            | col:column
            | constant:number 
            ;

        number = /[-]?[0-9]+[.]?[0-9]*/ ;
        string = /[a-zA-Z0-9_\.\-\/\,\:]+/ ;
        column = /[a-zA-Z][a-zA-Z0-9_]*/ ;
    '''


    model = tatsu.compile(GRAMMAR)


    def __init__(self, query):
        try:            
            self.ast = Parser.model.parse(query)
        except tatsu.exceptions.FailedParse as e:
            raise ParserException("Failed to parse query: {}".format(e))
        except tatsu.exceptions.FailedLeftRecursion as e:
            raise ParserException("Failed to parse query: {}".format(e))


    def get_features(self, ast=None):
        try:
            ast = ast if ast else self.ast
            if ast["q"]:
                return self.get_features(ast["q"])
            elif ast["t"]:
                return self.get_features(ast["t"])
            elif ast["qop"] or ast["top"]:
                return self.get_features(ast["left"]) | self.get_features(ast["right"])
            elif ast["cop"] and ast["ter"]:
                return { ast["col"] } | self.get_features(ast["ter"])
            elif ast["col"]:
                return { ast["col"] }
            else: 
                return set()
        except TypeError as e:
            print(ast)
            raise ParserException("Failed to parse query: {}".format(e))


    def build_where_recursive(self, db, ast=None):
        try:
            ast = ast if ast else self.ast
            if ast["q"]:
                return "({})".format(self.build_where_recursive(db, ast["q"]))
            elif ast["t"]:
                return "({})".format(self.build_where_recursive(db, ast["t"]))
            elif ast["qop"] or ast["top"]:
                operator = ast["qop"] if ast["qop"] else ast["top"]
                left = self.build_where_recursive(db, ast["left"])
                right = self.build_where_recursive(db, ast["right"])
                return "{} {} {}".format(left, operator, right)
            elif ast["cop"]:
                operator = "not like" if ast["cop"] == "unlike" else ast["cop"]
                feat = db.faddr_column(ast["col"])
                if ast["num"]:
                    return "{} {} {}".format(feat, operator, ast["num"])
                elif ast["str"]:
                    return "{} {} '{}'".format(feat, operator, ast["str"])
                elif ast["lik"]:
                    return "{} {} {}".format(feat, operator, ast["lik"])
                elif ast["ter"]:
                    return "{} {} {}".format(feat, operator, self.build_where_recursive(db, ast["ter"]))
            elif ast["col"]:
                feature = db.faddr_column(ast["col"])
                return "CAST({} AS FLOAT)".format(feature)
            elif ast["constant"]:
                return ast["constant"]
        except TypeError as e:
            print(self.ast)
            print(ast)
            raise ParserException("Failed to parse query: {}".format(e))
        except DatabaseException as e:
            print(self.ast)
            print(ast)
            raise ParserException("Failed to parse query: {}".format(e))