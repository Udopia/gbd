from tatsu import parse


def find_hashes(database, query=None):
    if query is None:
        return database.value_query("SELECT hash FROM benchmarks")
    else:
        ast = parse(GRAMMAR, query)
        where = build_where(ast)
        tables = build_join(ast)
        tables.discard("benchmarks")
        joined = "benchmarks "
        for table in tables:
            joined += "left join " + table + " on benchmarks.hash = " + table + ".hash "
        return database.value_query("SELECT benchmarks.hash FROM {} WHERE {}".format(joined, where))


def resolve(database, cat, hash):
    return database.value_query("SELECT value FROM {} WHERE hash='{}'".format(cat, hash))


GRAMMAR = r'''
    @@grammar::EXP
    @@ignorecase::True

    start = exp:expression $ ;

    expression
        =
        | '(' exp:expression ')'
        | exp1:expression con:('and' | 'or') exp2:expression
        | constraint
        ;

    constraint = attr:name op:('=' | '<' | '>' | '<=' | '>=' | '!=' | '<>' | 'like') val:value ;

    value
        =
        | num:numeric
        | alnum:alphanumeric
        ;

    numeric = /[0-9\.\-]+/ ;
    alphanumeric = /[a-zA-Z0-9_\-\%]+/ ;

    name = /[a-zA-Z0-9_]+/ ;
'''


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


def build_join(ast):
    result = set()
    if ast["exp"] is not None:
        result.update(build_join(ast["exp"]))
    elif ast["con"] is not None:
        result.update(build_join(ast["exp1"]))
        result.update(build_join(ast["exp2"]))
    elif ast["op"] is not None:
        result.add(ast["attr"])
    return result
