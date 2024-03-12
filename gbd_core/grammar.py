
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

from gbd_core.database import Database, DatabaseException

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
            | qop:("not") ~ q:query
            | constraint 
            | "(" q:query ")" 
            ;

        constraint 
            = 
            | col:(dbname ":" column | column) cop:("=" | "!=" | "<=" | ">=" | "<" | ">" ) ter:termstart
            | col:(dbname ":" column | column) cop:("=" | "!=" | "<=" | ">=" | "<" | ">" ) str:string 
            | col:(dbname ":" column | column) cop:("like" | "unlike") ~ lik:(["%"] string ["%"])
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
            col:(dbname ":" column | column)
            ;

        string
            =
            | "'" @:singlequotedstring "'"
            | '"' @:doublequotedstring '"'
            | /[a-zA-Z0-9_\.\-\/\,\:\+\=\@]+/
            ;

        number = /[-]?[0-9]+[.]?[0-9]*/ ;
        singlequotedstring = /[a-zA-Z0-9_\.\-\/\,\:\+\=\@\s"\*\\]+/ ;
        doublequotedstring = /[a-zA-Z0-9_\.\-\/\,\:\+\=\@\s'\*\\]+/ ;
        column = /[a-zA-Z][a-zA-Z0-9_]*/ ;
        dbname = /[a-zA-Z][a-zA-Z0-9_]*/ ;
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
        #import pprint
        #pp = pprint.PrettyPrinter(depth=6)
        #pp.pprint(ast)
        try:
            ast = ast if ast else self.ast
            if "q" in ast:
                return self.get_features(ast["q"])
            elif "t" in ast:
                return self.get_features(ast["t"])
            elif "qop" in ast or "top" in ast:
                return self.get_features(ast["left"]) | self.get_features(ast["right"])
            elif "cop" in ast and "ter" in ast:
                return { "".join(ast["col"]) } | self.get_features(ast["ter"])
            elif "col" in ast:
                return { "".join(ast["col"]) }
            else: 
                return set()
        except TypeError as e:
            raise ParserException("Failed to parse query: {}".format(str(e)))


    def get_sql(self, db: Database, ast=None):
        try:
            ast = ast if ast else self.ast
            if "qop" in ast and ast["qop"] == "not":
                return "NOT (" + self.get_sql(db, ast["q"]) + ")"
            elif "q" in ast:
                return "(" + self.get_sql(db, ast["q"]) + ")"
            elif "t" in ast:
                return "(" + self.get_sql(db, ast["t"]) + ")"
            elif "qop" in ast or "top" in ast: # query operator or term operator
                operator = ast["qop"] if ast["qop"] else ast["top"]
                left = self.get_sql(db, ast["left"])
                right = self.get_sql(db, ast["right"])
                return "{} {} {}".format(left, operator, right)
            elif "cop" in ast: # constraint operator
                operator = "not like" if ast["cop"] == "unlike" else ast["cop"]
                feat = db.faddr("".join(ast["col"]))
                feat_is_1_n = db.find("".join(ast["col"])).default is None
                if "str" in ast: # cop:("=" | "!=")
                    if feat_is_1_n:
                        table = db.faddr_table("".join(ast["col"]))
                        setop = "IN" if ast["cop"] == "=" else "NOT IN"
                        return "{t}.hash {o} (SELECT {t}.hash FROM {t} WHERE {f} = '{s}')".format(o=setop, t=table, f=feat, s=ast["str"])
                    else:
                        return "{} {} '{}'".format(feat, operator, ast["str"])
                elif "lik" in ast: # cop:("like" | "unlike")
                    if feat_is_1_n:
                        table = db.faddr_table("".join(ast["col"]))
                        setop = "IN" if ast["cop"] == "like" else "NOT IN"
                        return "{t}.hash {o} (SELECT {t}.hash FROM {t} WHERE {f} like '{s}')".format(o=setop, t=table, f=feat, s="".join([ t for t in ast["lik"] if t ]))
                    else:
                        return "{} {} '{}'".format(feat, operator, "".join([ t for t in ast["lik"] if t ]))
                elif "ter" in ast: # cop:("=" | "!=" | "<=" | ">=" | "<" | ">" )
                    if feat_is_1_n and ast["cop"] == "!=":
                        table = db.faddr_table("".join(ast["col"]))
                        setop = "NOT IN" if ast["cop"] == "!=" else "IN"
                        cop = "=" if ast["cop"] == "!=" else ast["cop"]
                        return "{t}.hash {o} (SELECT {t}.hash FROM {t} WHERE CAST({f} AS FLOAT) {c} {s})".format(o=setop, c=cop, t=table, f=feat, s=self.get_sql(db, ast["ter"]))
                    else:
                        return "CAST({} AS FLOAT) {} {}".format(feat, operator, self.get_sql(db, ast["ter"]))
                raise ParserException("Missing right-hand side of constraint")
            elif "col" in ast:
                feature = db.faddr("".join(ast["col"]))
                return "CAST({} AS FLOAT)".format(feature)
            elif "constant" in ast:
                return ast["constant"]
            else:
                return "1=1"
        except TypeError as e:
            raise ParserException("Failed to parse query: {}".format(str(e)))
        except DatabaseException as e:
            raise ParserException("Failed to parse query: {}".format(str(e)))

