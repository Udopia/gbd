import csv
import re

from main.gbd_tool.database import groups, benchmark_administration


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


def get_header(filename, key_column):
    fieldnames = []
    with open(filename, newline='') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter=',', quotechar='\'')
        fieldnames = [field for field in csvreader.fieldnames if field != key_column]
    return fieldnames


def create_group(database, filename, csv_column, db_column):
    with open(filename, newline='') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter=' ', quotechar='\'')
        values = [line[csv_column] for line in csvreader]
        sqltype = determine_type(database, values)
        print('Column {} has type {} [values: {}, {}, {}, ...]'.format(csv_column, sqltype, values[0], values[1],
                                                                       values[2]))
        groups.add(database, db_column, unique=True, type=sqltype, default=None)


def import_csv(database, filename, key, source, target):
    with open(filename, newline='') as csvfile:
        if not exists(database, target):
            print("Creating table {}".format(target))
            create_group(database, csvfile, source, target)
            csvreader = csv.DictReader(csvfile, delimiter=' ', quotechar='\'')
            lst = [(row[key], row[source]) for row in csvreader if row[source].strip()]
            print("Inserting {} values into table {}".format(len(lst), target))
            database.bulk_insert(target, lst)
        else:
            csvreader = csv.DictReader(csvfile, delimiter=' ', quotechar='\'')
            lst = [(row[key], row[source]) for row in csvreader if row[source].strip()]
            print("Attempting to insert {} values into table {}".format(len(lst), target))
            for (hash_, value_) in lst:
                benchmark_administration.add_tag(database, target, value_, hash_, False)
