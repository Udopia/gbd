import db
import groups
import os

def add_tag(database, cat, tag, hash):
  info = groups.reflect(database, cat)
  if (info[0]['unique']):
    database.submit('REPLACE INTO {} (hash, value) VALUES ("{}", "{}")'.format(cat, hash, tag))
  else:
    res = database.value_query("SELECT hash FROM {} WHERE hash='{}' AND value='{}'".format(cat, hash, tag))
    if (len(res) == 0):
      database.submit('INSERT INTO {} (hash, value) VALUES ("{}", "{}")'.format(cat, hash, tag))

def remove_tag(database, cat, tag, hash):
  database.submit("DELETE FROM {} WHERE hash='{}' AND value='{}'".format(cat, hash, tag))


def add_benchmark(database, hash, path):
  database.submit('INSERT INTO benchmarks (hash, value) VALUES ("{}", "{}")'.format(hash, path))
  g = groups.reflect(database)
  for group in g:
    info = groups.reflect(database, group)
    dval = info[1]['default_value']
    if (dval is not None):
      database.submit('INSERT OR IGNORE INTO {} (hash) VALUES ("{}")'.format(group, hash))
