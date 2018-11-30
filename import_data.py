import csv
import re
import groups
import tags

def exists(database, cat):
  g = groups.reflect(database)
  return (cat in g)

def determine_type(database, values):
  is_real = False
  re_int = re.compile('[0-9]+')
  re_double = re.compile('[0-9]+\.[0-9]+')
  for value in values:
    value = value.strip()
    if not value:
      continue
    if re_int.fullmatch(value) is None:
      is_real = True
      if re_double.fullmatch(value) is None:
        return "text"
  return "integer" if not is_real else "real"

def get_header(csv_file, key_column):
  fieldnames = []
  with open(csv_file, newline='') as csvfile:
    csvreader = csv.DictReader(csvfile, delimiter=',', quotechar='\'')
    fieldnames = [field for field in csvreader.fieldnames if field != key_column]
  return fieldnames

def create_group(database, csv_file, db_column, csv_column):
  with open(csv_file, newline='') as csvfile:
    csvreader = csv.DictReader(csvfile, delimiter=',', quotechar='\'')
    values = [line[csv_column] for line in csvreader]
    sqltype = determine_type(database, values)
    print('Column {} has type {} [values: {}, {}, {}, ...]'.format(csv_column, sqltype, values[0], values[1], values[2]))
    groups.add(database, db_column, unique=True, type=sqltype, default=None)

def import_csv(database, csv_file, key_column, column_names, column_prefix=""):
  for column in column_names:
    db_column = "{}{}".format(column_prefix, column)
    if not exists(database, db_column):
      pat = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]*$")
      if not pat.match(db_column):
        raise ValueError("Column name {} does not match regular expression".format(db_column))
      create_group(database, csv_file, db_column, column)
    with open(csv_file, newline='') as csvfile:
      print("Inserting into table {} ... ".format(db_column))
      csvreader = csv.DictReader(csvfile, delimiter=' ', quotechar='\'')
      lst = [(row[key_column], row[column]) for row in csvreader if row[column].strip()]
      print("Inserting {} values into table {}".format(len(lst), db_column))
      database.bulk_insert(db_column, lst)