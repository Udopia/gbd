from main import groups, search
from main.database.db import Database

from flask import Flask, render_template, request

app = Flask(__name__)

from os.path import realpath, dirname, join

DATABASE = join(dirname(realpath(__file__)), 'local.db')


@app.route("/", methods={'GET'})
def welcome():
    return render_template('home.html')  # file is found at runtime


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


@app.route("/query", methods=['GET', 'POST'])   # query string über post
def query():
    param = dict()
    if request.method == 'POST':
        param = request.values.to_dict()
    else:
        param = request.args.to_dict()
    result = ""
    with Database(DATABASE) as database:
        if ("query" in param):
            hashes = search.find_hashes(database, param["query"])
            for h in hashes:
                result += "{}\n".format(h)
        elif (len(param.keys()) == 0):
            hashes = search.find_hashes(database, "benchmarks")
            for h in hashes:
                result += "{}\n".format(h)
        else:
            for attribute, value in param.items():
                if (attribute is not None and value is not None):
                    hashes = search.find_hashes(database, attribute, value)
                    for h in hashes:
                        result += "{}\n".format(h)
    return result
