import csv
import re
import db

def add(database, cat, unique=False, type='text', default=None):
  ustr = "UNIQUE" if unique else ""
  dstr = "DEFAULT {}".format(default) if default is not None else ""
  database.submit('CREATE TABLE IF NOT EXISTS {} (hash TEXT {} NOT NULL, value {} NOT NULL {})'.format(cat, ustr, type, dstr))
  if default is not None:
    database.submit('INSERT OR IGNORE INTO {} (hash) SELECT hash FROM benchmarks'.format(cat))

def remove(database, cat):
  database.submit('DROP TABLE IF EXISTS {}'.format(cat))

def clear(database, cat):
  database.submit('DELETE FROM {}'.format(cat))

def reflect(database, cat=None):
  if cat is None:
    lst = database.query("SELECT tbl_name FROM sqlite_master WHERE type='table'")
    groups = [x[0] for x in lst]
    return groups
  else:
    lst = database.query("PRAGMA table_info({})".format(cat))
    columns = ( 'index', 'name', 'type', 'notnull', 'default_value', 'pk' )
    table_infos = [dict(zip(columns, values)) for values in lst]

    lst = database.query("PRAGMA index_list({})".format(cat))
    columns = ( 'seq', 'name', 'unique', 'origin', 'partial' )
    index_list = [dict(zip(columns, values)) for values in lst]

    # create key default
    for info in table_infos:
      info['unique'] = 0

    # determine unique columns
    for values in index_list:
      tup = database.query("PRAGMA index_info({})".format(values['name']))
      columns = ( 'index_rank', 'table_rank', 'name' )
      index_info = dict(zip(columns, tup[0]))
      colid = index_info['table_rank']
      table_infos[colid]['unique'] = values['unique']

    return table_infos

def reflect_tags(database, cat):
  return database.value_query('SELECT DISTINCT value FROM {}'.format(cat))

def reflect_size(database, cat):
  return database.value_query('SELECT count(*) FROM {}'.format(cat))

def reflect_unique(database, cat):
  info = reflect(database, cat)
  return info[0]['unique']

def reflect_default(database, cat):
  info = reflect(database, cat)
  return info[1]['default_value']

def reflect_type(database, cat):
  info = reflect(database, cat)
  return info[1]['type']

def exists(database, cat):
  groups = reflect(database)
  return (cat in groups)

def determine_type(database, values):
  real = False
  rint = re.compile('[0-9]+')
  rdouble = re.compile('[0-9]+\.[0-9]+')
  for value in values:
    value = value.strip()
    if rint.fullmatch(value) is None:
      real = True
      if rdouble.fullmatch(value) is None:
        return "text"
  return "integer" if not real else "real"

def create_groups(database, file, column_prefix="", key_column="instance"):
  fieldnames = []
  with open(file, newline='') as csvfile:
    csvreader = csv.DictReader(csvfile, delimiter=',', quotechar='\'')
    fieldnames = [field for field in csvreader.fieldnames if field != key_column]
  for field in fieldnames:
    with open(file, newline='') as csvfile:
      csvreader = csv.DictReader(csvfile, delimiter=',', quotechar='\'')
      if not exists(database, field):
        values = [line[field] for line in csvreader]
        sqltype = determine_type(database, values)
        print('Column {} has type {} [values: {}, {}, {}, ...]'.format(field, sqltype, values[0], values[1], values[2]))
