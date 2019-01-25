import htmlGenerator

from tatsu import exceptions
from core.main import groups, search
from core.database.db import Database
from sqlite3 import OperationalError


from flask import Flask, render_template, request, url_for

app = Flask(__name__)

from os.path import realpath, dirname, join

DATABASE = join(dirname(realpath(__file__)), 'local.db')


@app.route("/", methods={'GET'})
def welcome():
    return render_template('home.html')


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


@app.route("/resolve_form", methods=['GET'])
def test():
    return render_template('resolve_form.html')


@app.route("/query_form", methods=['GET'])
def query_form():
    return render_template('query_form.html')


@app.route("/query", methods=['POST'])   # query string über post
def query():
    response = htmlGenerator.generate_html_header("en")
    response += htmlGenerator.generate_head("Results")
    query = request.values.to_dict()["query"]
    with Database(DATABASE) as database:
        try:
            list = search.find_hashes(database, query)
            response += htmlGenerator.generate_num_table_div(list)
        except exceptions.FailedParse:
            response += htmlGenerator.generate_warning("Non-valid query")
        except OperationalError:
            response += htmlGenerator.generate_warning("Group not found")

    return response

