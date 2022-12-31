# GBD Benchmark Database (GBD)
# Copyright (C) 2022 Markus Iser, Karlsruhe Institute of Technology (KIT)
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import tatsu


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
            | left:query ~ qop:('and' | 'or') ~ right:query 
            | sconstraint 
            | aconstraint 
            | '(' q:query ')' 
            ;

        sconstraint 
            = 
            | left:column sop:('=' | '!=') right:string
            | left:column sop:('unlike' | 'like') ~ ["%"] right:string ["%"]
            ;
            
        aconstraint 
            = 
            left:term aop:('=' | '!=' | '<=' | '>=' | '<' | '>' ) right:term 
            ;

        term 
            = 
            | value:column 
            | constant:number 
            #| '(' left:term top:('+'|'-'|'*'|'/') right:term ')' 
            ;

        number = /[0-9\.\-]+/ ;
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
        ast = ast if ast else self.ast
        if ast["q"]:
            return self.get_features(ast["q"])
        elif ast["qop"] or ast["aop"] or ast["top"]:
            return self.get_features(ast["left"]) | self.get_features(ast["right"])
        elif ast["sop"]:
            return { ast["left"] }
        elif ast["value"]:
            return { ast["value"] }
        else: 
            return set()


    def build_where_recursive(self, db, ast=None):
        ast = ast if ast else self.ast
        if ast["q"]:
            return self.build_where_recursive(db, ast["q"])
        elif ast["qop"]:
            operator = ast["qop"]
            left = self.build_where_recursive(db, ast["left"])
            right = self.build_where_recursive(db, ast["right"])
            return "({} {} {})".format(left, operator, right)
        elif ast["aop"]:
            operator = ast["aop"]
            left = self.build_where_recursive(db, ast["left"])
            right = self.build_where_recursive(db, ast["right"])
            return "{} {} {}".format(left, operator, right)
        elif ast["top"]:
            operator = ast["top"]
            left = self.build_where_recursive(db, ast["left"])
            right = self.build_where_recursive(db, ast["right"])
            return "{} {} {}".format(left, operator, right)
        elif ast["sop"]:
            operator = "not like" if ast["sop"] == "unlike" else ast["sop"]
            feature = ast["left"]
            feat = db.faddr_column(feature)
            right = ast["right"]
            return "{} {} \"{}\"".format(feat, operator, right)
        elif ast["value"]:
            feature = ast["value"]
            feat = db.faddr_column(feature)
            return "CAST({} AS FLOAT)".format(feat)
        elif ast["constant"]:
            return ast["constant"]