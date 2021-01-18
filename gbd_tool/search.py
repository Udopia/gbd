# Global Benchmark Database (GBD)
# Copyright (C) 2020 Markus Iser, Karlsruhe Institute of Technology (KIT)
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

from gbd_tool.util import eprint
from tatsu import parse
import pprint


def build_query(query=None, hashes=[], resolve=[], collapse="GROUP_CONCAT", group_by="hash", join_type="LEFT"):
    statement = "SELECT {} FROM {} {} WHERE {} GROUP BY {}"

    s_attributes = group_by + ".value"
    s_from = group_by
    s_tables = ""
    s_conditions = "1=1"
    s_group_by = group_by + ".value"
    tables = set(resolve)
    
    if query is not None and query:
        ast = parse(GRAMMAR, query)        
        s_conditions = build_where(ast)
        tables.update(collect_tables(ast))

    if len(hashes):
        s_conditions = s_conditions + " AND hash.hash in ('{}')".format("', '".join(hashes))

    if len(resolve):
        s_attributes = s_attributes + ", " + ", ".join(['{}(DISTINCT({}.value))'.format(collapse, table) for table in resolve])

    s_tables = " ".join(['{} JOIN {} ON {}.hash = {}.hash'.format(join_type, table, group_by, table) for table in tables if table != group_by])

    return statement.format(s_attributes, s_from, s_tables, s_conditions, s_group_by)


def build_where(ast):
    if ast["q"]:
        return build_where(ast["q"])
    elif ast["qop"]:
        return "({} {} {})".format(build_where(ast["left"]), ast["qop"], build_where(ast["right"]))
    elif ast["sop"]:
        return "{}.value {} \"{}\"".format(ast["left"], ast["sop"], ast["right"])
    elif ast["aop"]:
        return "{} {} {}".format(build_where(ast["left"]), ast["aop"], build_where(ast["right"]))
    elif ast["bracket_term"]:
        return "({})".format(build_where(ast["bracket"]))
    elif ast["top"]:
        return "{} {} {}".format(build_where(ast["left"]), ast["top"], build_where(ast["right"]))
    elif ast["value"]:        
        return "CAST({}.value AS FLOAT)".format(ast["value"])
    elif ast["constant"]:
        return ast["constant"]


def collect_tables(ast):
    if ast["q"]:
        return collect_tables(ast["q"])
    elif ast["qop"] or ast["aop"] or ast["top"]:
        return collect_tables(ast["left"]) | collect_tables(ast["right"])
    elif ast["bracket_term"]:
        return collect_tables(ast["bracket_term"])
    elif ast["sop"]:
        return { ast["left"] }
    elif ast["value"]:
        return { ast["value"] }
    else: 
        return set()


GRAMMAR = r'''
    @@grammar::EXP
    @@ignorecase::True

    start = q:query $ ;

    query = '(' q:query ')' | left:query qop:('and' | 'or') right:query | scon | acon;

    scon = left:colname sop:('=' | '!=') right:alnum | left:colname sop:('like') right:likean ;
        
    acon = left:term aop:('=' | '!=' | '<' | '>' | '<=' | '>=' ) right:term ;

    term = value:colname | constant:num | '(' left:term top:('+'|'-'|'*'|'/') right:term ')' ;

    num = /[0-9\.\-]+/ ;
    alnum = /[a-zA-Z0-9_\.\-\/\?]+/ ;
    likean = /[\%]?[a-zA-Z0-9_\.\-\/\?]+[\%]?/;
    colname = /[a-zA-Z][a-zA-Z0-9_]+/ ;
'''
