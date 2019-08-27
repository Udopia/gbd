from tatsu import parse


def find_hashes(database, query=None, resolve=None):
    if query is None:
        return database.value_query("SELECT hash FROM benchmarks")
    else:
        ast = parse(GRAMMAR, query)
        tables = collect_tables(ast)
        tables.discard("benchmarks")
        if resolve is None or len(resolve) == 0:
            sql_select = "benchmarks.hash, GROUP_CONCAT(benchmarks.value)"
        elif "hash" in resolve:
            resolve.remove("hash") 
            tables.update(resolve)
            sql_select = ", ".join(['GROUP_CONCAT({}.value)'.format(table) for table in resolve])
            sql_select = "benchmarks.hash, {}".format(sql_select)
        else:
            tables.update(resolve)
            sql_select = ", ".join(['{}.value'.format(table) for table in resolve])
        where = build_where(ast)
        tables.discard("benchmarks")
        sql_from = " ".join(['LEFT JOIN {} ON benchmarks.hash = {}.hash'.format(table, table) for table in tables])
        statement = "SELECT {} FROM benchmarks {} WHERE {} GROUP BY benchmarks.hash".format(sql_select, sql_from, where)
        print (statement)
        return database.query(statement)


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
    alphanumeric = /[a-zA-Z0-9_\-\%\.\/]+/ ;

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
