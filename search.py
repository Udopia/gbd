
import db
import groups
import re

from tatsu import parse
from tatsu.util import asjson

def find_hashes(database, cat):
  return database.value_query("SELECT hash FROM {}".format(cat))

def find_hash(database, cat, tags):
  is_numeric = groups.reflect_type(database, cat).lower() != 'text'

  regex = r"^([<>=]?=?)(\-?[\.0-9]+)$" if is_numeric else r"^([<>=]=?)?([^ \t\n\r\f\v]*)$"

  constraints = []
  for tag in tags:
    match = re.search(regex, tag)
    if match is None:
      raise ValueError("Argument {} does not match pattern '{}', which is required for group {}".format(tag, regex, cat))

    default_operator = "=" if is_numeric else "LIKE"
    operator = match.group(1) or default_operator

    prefix = '"' if match.group(1) is not None else '"%'
    postfix = '"' if match.group(1) is not None else '%"'
    value = match.group(2) if is_numeric else prefix + match.group(2) + postfix

    constraints.append("value {} {}".format(operator, value))

  where = ' AND '.join(["{}"]*len(constraints)).format(*constraints)
  sql = "SELECT hash FROM {} WHERE {}".format(cat, where)
  result = database.value_query(sql)

  return result


GRAMMAR = '''
    @@grammar::EXP
    @@ignorecase::True

    start = exp:expression $ ;

    expression
        =
        | '(' exp:expression ')'
        | exp1:expression con:('and' | 'or') exp2:expression
        | constraint
        ;

    constraint = attr:name op:('=' | '<' | '>' | '<=' | '>=' | 'like') val:value ;

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

def find_hashes_by_query(database, query):
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
