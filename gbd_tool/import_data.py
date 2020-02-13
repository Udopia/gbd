# Global Benchmark Database (GBD)
# Copyright (C) 2019 Markus Iser, Luca Springer, Karlsruhe Institute of Technology (KIT)
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

import csv
import re

from gbd_tool import groups, benchmark_administration


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


def create_group(database, filename, header, csv_column, db_column, delim_=' '):
    with open(filename, newline='') as csvfile:
        csvfile.readline() #skip header
        csvreader = csv.DictReader(csvfile, fieldnames=header, delimiter=delim_, quotechar='\'')
        # print("Importing: {}".format(csvreader.fieldnames))
        values = [line[csv_column].strip() for line in csvreader]
        sqltype = determine_type(database, values)
        print('Column {} has type {} [values: {}, {}, {}, ...]'.format(csv_column, sqltype, values[0], values[1],
                                                                       values[2]))
        groups.add(database, db_column, unique=True, type=sqltype, default=None)


def import_csv(database, filename, key, source, target, delim_=' '):
    with open(filename, newline='') as csvfile:
        # trim whitespace off header
        header = [h.strip() for h in csvfile.readline().split(delim_)]
        print(header)
        if not exists(database, target):
            print("Creating table {}".format(target))
            create_group(database, filename, header, source, target, delim_)
            csvreader = csv.DictReader(csvfile, fieldnames=header, delimiter=delim_, quotechar='\'')
            lst = [(row[key], row[source].strip()) for row in csvreader if row[source]]
            print("Inserting {} values into table {}".format(len(lst), target))
            database.bulk_insert(target, lst)
        else:
            csvreader = csv.DictReader(csvfile, fieldnames=header,  delimiter=delim_, quotechar='\'')
            lst = [(row[key], row[source].strip()) for row in csvreader if row[source]]
            print("Attempting to insert {} values into table {}".format(len(lst), target))
            for (hash_, value_) in lst:
                benchmark_administration.add_tag(database, target, value_, hash_, False)
