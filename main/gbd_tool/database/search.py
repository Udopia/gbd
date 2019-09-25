from tatsu import parse
from gbd_tool.util import eprint


def find_hashes(database, query=None, resolve=[]):
    statement = "SELECT DISTINCT {} FROM benchmarks {} WHERE {} GROUP BY benchmarks.hash"
    s_attributes = "benchmarks.hash"
    s_tables = ""
    s_conditions = "1=1"
    tables = { "benchmarks" }
    
    if query is not None:
        ast = parse(GRAMMAR, query)
        s_conditions = build_where(ast)
        tables.update(collect_tables(ast))

    if resolve is not None:
        if len(resolve) == 0:
            resolve.append("benchmarks")
        s_attributes = "benchmarks.hash, " + ", ".join(['GROUP_CONCAT(DISTINCT({}.value))'.format(table) for table in resolve])
        tables.update(resolve)

    s_tables = " ".join(['LEFT JOIN {} ON benchmarks.hash = {}.hash'.format(table, table) for table in tables if table != "benchmarks"])

    eprint(statement.format(s_attributes, s_tables, s_conditions))

    return database.query(statement.format(s_attributes, s_tables, s_conditions))


def build_where(ast):
    result = ""
    if ast["exp"] is not None:
        result = build_where(ast["exp"])
    elif ast["con"] is not None:
        result = "(" + build_where(ast["exp1"]) + " " + ast["con"] + " " + build_where(ast["exp2"]) + ")"
    elif ast["op"] is not None:
        value = ast["val"]["num"] or "'" + ast["val"]["alnum"] + "'"
        result = ast["attr"] + ".value " + ast["op"] + " " + value
    return result


def collect_tables(ast):
    result = set()
    if ast["exp"] is not None:
        result.update(collect_tables(ast["exp"]))
    elif ast["con"] is not None:
        result.update(collect_tables(ast["exp1"]))
        result.update(collect_tables(ast["exp2"]))
    elif ast["op"] is not None:
        result.add(ast["attr"])
    return result


GRAMMAR = r'''
    @@grammar::EXP
    @@ignorecase::True

    start = exp:expression $ ;

    expression
        = '(' exp:expression ')'
        | exp1:expression con:('and' | 'or') exp2:expression
        | constraint
        ;

    constraint = attr:name op:('=' | '<' | '>' | '<=' | '>=' | '!=' | '<>' | 'like') val:value ;

    value
        = num:numeric
        | alnum:alphanumeric
        ;

    numeric = /[0-9\.\-]+/ ;
    alphanumeric = /[a-zA-Z0-9_\-\%\.\/]+/ ;

    name = /[a-zA-Z0-9_]+/ ;
'''
