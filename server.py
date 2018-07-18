import groups
import search
from db import Database
import optparse

from flask import Flask, request
app = Flask(__name__)

from os.path import realpath, dirname, join, isfile
DATABASE = join(dirname(realpath(__file__)), 'local.db')

@app.route("/resolve/<hash>", methods=['GET'])
def resolve(hash):
  param = request.args.to_dict()
  result = ""
  with Database(DATABASE) as database:
    if len(param.keys()) == 0:
      attributes = groups.reflect(database)
    for name in param.keys():
      value = search.resolve(database, name, hash)
      result += "{} {}\n".format(name, *value)
  return result

@app.route("/query", methods=['GET', 'POST'])
def query():
  param = dict()
  if request.method == 'POST':
    param = request.values.to_dict()
  else:
    param = request.args.to_dict()
  result = ""
  with Database(DATABASE) as database:
    if ("query" in param):
      hashes = search.find_hashes_by_query(database, param["query"])
      for h in hashes:
        result += "{}\n".format(h)
    elif (len(param.keys()) == 0):
      hashes = search.find_hashes(database, "benchmarks")
      for h in hashes:
        result += "{}\n".format(h)
    else:
      for attribute, value in param.items():
        if (attribute is not None and value is not None):
          hashes = search.find_hash(database, attribute, value)
          for h in hashes:
            result += "{}\n".format(h)
  return result
