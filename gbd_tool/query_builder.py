# GBD Benchmark Database (GBD)
# Copyright (C) 2021 Markus Iser, Karlsruhe Institute of Technology (KIT)
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

from gbd_tool.db import Database, DatabaseException
from gbd_tool.util import eprint

class GBDQuery:
    GRAMMAR = r'''
        @@grammar::EXP
        @@ignorecase::True

        start = q:query $ ;

        query = left:query qop:('and' | 'or') right:query | 
                '(' q:query ')' | 
                sconstraint | 
                aconstraint;

        sconstraint = left:colname sop:('=' | '!=') right:alnum | 
                    left:colname sop:('=' | '!=') "'" right:alnumplus "'" | 
                    left:colname sop:('unlike' | 'like') right:likean ;
            
        aconstraint = left:term aop:('=' | '!=' | '<=' | '>=' | '<' | '>' ) right:term ;

        term = value:colname | constant:num | '(' left:term top:('+'|'-'|'*'|'/') right:term ')' ;

        num = /[0-9\.\-]+/ ;
        alnum = /[a-zA-Z0-9_\.\-\/]+/ ;
        alnumplus =   /[a-zA-Z0-9_\.\-\/\?\#\+\=\:\^\ ]+/ ;
        likean = /[\%]?[a-zA-Z0-9_\.\-\/\?\#\+\=\:\^\ ]+[\%]?/;
        colname = /[a-zA-Z][a-zA-Z0-9_]+/ ;
    '''

    def __init__(self, db: Database, join_type="LEFT", collapse="GROUP_CONCAT", where_in_subselect=False):
        self.db = db
        self.join_type = join_type
        self.subselect = where_in_subselect
        self.collapse = collapse if collapse != "none" else None

    # Generate SQL Query from given GBD Query 
    def build_query(self, query=None, hashes=[], resolve=[], group_by="hash"):
        ast = None if not query else tatsu.parse(self.GRAMMAR, query)

        self.features_exist_or_throw(resolve + [group_by])

        sel = self.build_select(group_by, resolve)
        fro = self.build_from(group_by)
        joi = self.build_join(group_by, self.collect_features(ast, resolve))
        whe = self.build_where(ast, hashes, group_by)

        gro = "GROUP BY {}".format(self.addr(group_by)) if self.collapse else " "
        
        return "SELECT {} FROM {} {} WHERE {} {} ORDER BY {}".format(sel, fro, joi, whe, gro, self.addr(group_by))


    def features_exist_or_throw(self, features):
        for feature in features:
            if not feature in self.db.get_features(tables=True, views=True):
                raise DatabaseException("Unknown feature '{}'".format(feature))

    def addr(self, feature):
        return "{}.{}".format(self.db.ftable(feature), self.db.fcolumn(feature))


    def build_select(self, group_by, resolve):
        result = self.addr(group_by)
        for res in resolve:
            res = self.addr(res)
            if self.collapse:
                res = "{}(DISTINCT({}))".format(self.collapse, res)
            result = result + ", " + res
        return result


    def build_from(self, group_by):
        return self.db.ftable(group_by)


    def build_join(self, group_by, features):
        group_context = self.db.fcontext(group_by)
        used_contexts = []
        used_tables = [ self.db.ftable(group_by) ]
        result = ""
        for feature in features:
            ftab = self.db.ftable(feature)
            if not ftab in used_tables:
                used_tables.append(ftab)
                gtab = self.db.ftable(group_by)
                feature_context = self.db.fcontext(feature)
                if feature_context == group_context:
                    if self.db.fdefault(feature) or self.db.fvirtual(feature):
                        result = result + " {} JOIN {} ON {}.hash = {}.hash".format(self.join_type, ftab, gtab, ftab)
                    else:
                        result = result + " {} JOIN {} ON {}.{} = {}.hash".format(self.join_type, ftab, gtab, feature, ftab)
                else:
                    translator = "{}_to_{}".format(group_context, feature_context)
                    dbtrans = self.db.ftable(translator)
                    if not translator in self.db.get_features():
                        raise DatabaseException("Context translator table not found: " + translator)
                    if not feature_context in used_contexts and not dbtrans in used_tables:
                        used_contexts.append(feature_context)
                        used_tables.append(dbtrans)
                        translator2 = "{}_to_{}".format(feature_context, group_context)
                        dbtrans2 = self.db.ftable(translator2)
                        result = result + " INNER JOIN {} ON {}.hash = {}.value".format(self.db.ftable(translator2), gtab, dbtrans2)
                    result = result + " INNER JOIN {} ON {}.hash = {}.hash".format(ftab, dbtrans2, ftab)
        return result

    def collect_features(self, ast, resolve):
        result = set(resolve)
        if ast:
            result.update(self.collect_features_recursive(ast))
        self.features_exist_or_throw(result)
        return result

    def collect_features_recursive(self, ast):
        if ast["q"]:
            return self.collect_features_recursive(ast["q"])
        elif ast["qop"] or ast["aop"] or ast["top"]:
            return self.collect_features_recursive(ast["left"]) | self.collect_features_recursive(ast["right"])
        elif ast["sop"]:
            return { ast["left"] }
        elif ast["value"]:
            return { ast["value"] }
        else: 
            return set()


    def build_where(self, ast, hashes, group_by):
        result = "{}.{} != 'None'".format(self.db.ftable(group_by), group_by)
        if ast:
            if self.subselect:
                fro = self.build_from(group_by)
                joi = self.build_join(group_by, self.collect_features_recursive(ast))
                whe = self.build_where_recursive(ast)
                result = result + " AND {}.hash in (SELECT {}.hash FROM {} {} WHERE {})".format(self.db.ftable(group_by), self.db.ftable(group_by), fro, joi, whe)
            else:
                result = result + " AND " + self.build_where_recursive(ast)
        if len(hashes):
            result = result + " AND {}.hash in ('{}')".format(self.db.ftable(group_by), "', '".join(hashes))
        return result

    def build_where_recursive(self, ast):
        if ast["q"]:
            return self.build_where_recursive(ast["q"])
        elif ast["qop"]:
            operator = ast["qop"]
            left = self.build_where_recursive(ast["left"])
            right = self.build_where_recursive(ast["right"])
            return "({} {} {})".format(left, operator, right)
        elif ast["aop"]:
            operator = ast["aop"]
            left = self.build_where_recursive(ast["left"])
            right = self.build_where_recursive(ast["right"])
            return "{} {} {}".format(left, operator, right)
        elif ast["top"]:
            operator = ast["top"]
            left = self.build_where_recursive(ast["left"])
            right = self.build_where_recursive(ast["right"])
            return "{} {} {}".format(left, operator, right)
        elif ast["sop"]:
            operator = "not like" if ast["sop"] == "unlike" else ast["sop"]
            feature = ast["left"]
            ftab = self.db.ftable(feature)
            fcol = self.db.fcolumn(feature)
            right = ast["right"]
            return "{}.{} {} \"{}\"".format(ftab, fcol, operator, right)
        elif ast["value"]:
            feature = ast["value"]
            ftab = self.db.ftable(feature)
            fcol = self.db.fcolumn(feature)
            return "CAST({}.{} AS FLOAT)".format(ftab, fcol)
        elif ast["constant"]:
            return ast["constant"]
