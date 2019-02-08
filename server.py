import random
import string

from tatsu import exceptions

from main import htmlGenerator
from main.core.main import groups, search
from main.core.database.db import Database
from sqlite3 import OperationalError


from flask import Flask, render_template, request

from zipfile import ZipFile, ZIP_DEFLATED
from os.path import realpath, dirname, join


app = Flask(__name__)

DATABASE = join(dirname(realpath(__file__)), 'local.db')


@app.route("/", methods={'GET'})
def welcome():
    return render_template('home.html')


@app.route("/query/form", methods=['GET'])
def query_form():
    return render_template('query_form.html')


@app.route("/query", methods=['POST'])   # query string über post
def query():
    response = htmlGenerator.generate_html_header("en")
    response += htmlGenerator.generate_head("Results")
    query = request.values.to_dict()["query"]
    with Database(DATABASE) as database:
        try:
            hashlist = search.find_hashes(database, query)
            response += htmlGenerator.generate_num_table_div(hashlist)
            if len(hashlist) != 0:
                name = 'serverstore/'
                name += ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(10))
                name += '.zip'
                with ZipFile(name, 'w', ZIP_DEFLATED) as myzip:
                    for h in hashlist:
                        myzip.write(*search.resolve(database, "benchmarks", h))
                response += htmlGenerator.generate_zip_download(name)
        except exceptions.FailedParse:
            response += htmlGenerator.generate_warning("Non-valid query")
        except OperationalError:
            response += htmlGenerator.generate_warning("Group not found")
    return response


@app.route("/resolve/form", methods=['GET'])
def resolve_form():
    return render_template('resolve_form.html')


@app.route("/resolve", methods=['POST'])
def resolve():
    hashed = request.values.to_dict()["hash"]
    group = request.values.to_dict()["group"]
    result = htmlGenerator.generate_html_header("en")
    result += htmlGenerator.generate_head("Results")
    with Database(DATABASE) as database:
        entries = []
        if group == "":
            allgroups = groups.reflect(database)
            for attribute in allgroups:
                if attribute != "__version":
                    value = search.resolve(database, attribute, hashed)
                    entries.append([attribute, value])
            result += htmlGenerator.generate_resolve_table_div(entries)
        else:
            try:
                value = search.resolve(database, group, hashed)
                entries.append([group, value])
                result += htmlGenerator.generate_resolve_table_div(entries)
            except OperationalError:
                result += htmlGenerator.generate_warning("Group not found")
            except IndexError:
                result += htmlGenerator.generate_warning("Hash not found in our database")
    return result


@app.route("/groups/all", methods=['GET'])
def reflect():
    with Database(DATABASE) as database:
        list = groups.reflect(database)
        return list.__str__()


@app.route("/groups/reflect/<group>", methods=['GET'])
def reflect_group(group):
    with Database(DATABASE) as database:
        try:
            trimmed = group.strip('<>')
            list = ["Name: {}".format(trimmed),
                    "Type: {}".format(groups.reflect_type(database, trimmed)),
                    "Unique: {}".format(groups.reflect_unique(database, trimmed)),
                    "Default: {}".format(groups.reflect_default(database, trimmed)),
                    "Size: {}".format(groups.reflect_size(database, trimmed))]
            return list.__str__()
        except IndexError:
            return "Group not found"
