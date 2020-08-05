#!/usr/bin/python3
# -*- coding: utf-8 -*-

# Global Benchmark Database (GBD)
# Copyright (C) 2020 Markus Iser, Luca Springer, Karlsruhe Institute of Technology (KIT)
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

import datetime
import logging
import os
import argparse

import werkzeug

from gbd_tool.util import eprint
from os.path import basename

import gbd_server

import tatsu
import flask
from flask import Flask, request, send_file, json, Response
from flask import render_template
from flask.logging import default_handler
from gbd_tool.gbd_api import GbdApi
from tatsu import exceptions
from werkzeug.middleware.proxy_fix import ProxyFix

app = Flask(__name__)

@app.route("/", methods=['GET'])
def quick_search():
    return render_template('index.html')

@app.route("/results", methods=['POST'])
def quick_search_results():
    with GbdApi(app.config['database']) as gbd_api:
        query = request.form.get('query')
        selected_features = list(filter(lambda x: x != '', request.form.get('selected_features').split(',')))
        if not len(selected_features):
            selected_features.append("filename")
        available_features = sorted(gbd_api.get_features())
        available_features.remove("local")
        features = sorted(list(set(available_features) & set(selected_features)))
        try:
            rows = list(gbd_api.query_search(query, features))
            features.insert(0, "GBDhash")
            result = list(dict((features[index], row[index]) for index in range(0, len(features))) for row in rows)
            return Response(json.dumps(result), status=200, mimetype="application/json")
        except tatsu.exceptions.FailedParse:
            return Response("Malformed query", status=400, mimetype="text/plain")
        except ValueError:
            return Response("Attribute not Available", status=400, mimetype="text/plain")


@app.route("/getdatabases", methods=["GET"])
def get_databases():
    with GbdApi(app.config['database']) as gbd_api:
        return json.dumps(gbd_api.get_databases())

@app.route('/getfeatures', defaults={'database': None})
@app.route('/getfeatures/<database>')
def get_features(database):
    with GbdApi(app.config['database']) as gbd_api:
        if database is None:
            available_features = sorted(gbd_api.get_features())
            available_features.remove("local")
            return Response(json.dumps(available_features), status=200, mimetype="application/json")
        elif database not in gbd_api.get_databases():
            return Response("Database does not exist in the running instance of GBD server", status=404,
                            mimetype="text/plain")
        else:
            return gbd_api.get_features(database)


@app.route("/exportcsv", methods=['POST'])
def get_csv_file():
    with GbdApi(app.config['database']) as gbd_api:
        query = request.form.get('query')
        selected_features = list(filter(lambda x: x != '', request.form.get('selected_features').split(',')))
        if not len(selected_features):
            selected_features.append("filename")
        results = gbd_api.query_search(query, selected_features)
        headers = ["hash"] + selected_features
        content = "\n".join([" ".join([str(entry) for entry in result]) for result in results])
        app.logger.info('Sending CSV file to {}'.format(request.remote_addr))
        file_name = "query_result.csv"
        return Response(" ".join(headers) + "\n" + content, mimetype='text/csv',
                        headers={"Content-Disposition": "attachment; filename=\"{}\"".format(file_name),
                                "filename": "{}".format(file_name)})


@app.route("/getinstances", methods=['POST'])
def get_url_file():
    with GbdApi(app.config['database']) as gbd_api:
        query = request.form.get('query')
        result = gbd_api.query_search(query, ["filename"])
        content = "\n".join([flask.url_for("get_file", hashvalue=row[0], filename=row[1], _external=True) for row in result])
        app.logger.info('Sending URL file to {}'.format(request.remote_addr))
        file_name = "query_result.uri"
        return Response(content, mimetype='text/uri-list',
                        headers={"Content-Disposition": "attachment; filename=\"{}\"".format(file_name),
                                "filename": "{}".format(file_name)})


@app.route('/attribute/<attribute>/<hashvalue>')
def get_attribute(attribute, hashvalue):
    with GbdApi(app.config['database']) as gbd_api:
        try:
            values = gbd_api.search(attribute, hashvalue)
            if len(values) == 0:
                return Response("No entry in attribute table associated with this hash", status=404, mimetype="text/plain")
            return str(",".join(str(value) for value in values))
        except ValueError as err:
            return Response("Value Error: {}".format(err), status=500, mimetype="text/plain")


@app.route('/file/<hashvalue>', defaults={'filename': None})
@app.route('/file/<hashvalue>/<filename>')
def get_file(hashvalue, filename):
    with GbdApi(app.config['database']) as gbd_api:
        values = gbd_api.search("local", hashvalue)
        if len(values) == 0:
            return Response("No according file found in our database", status=404, mimetype="text/plain")
        try:
            path = values.pop()
            return send_file(path, as_attachment=True, attachment_filename=os.path.basename(path))
        except FileNotFoundError:
            return Response("Files temporarily not accessible", status=404, mimetype="text/plain")


@app.route('/info/<hashvalue>')
def get_all_attributes(hashvalue):
    with GbdApi(app.config['database']) as gbd_api:
        features = gbd_api.get_features()
        info = dict([])
        for feature in features:
            values = gbd_api.search(feature, hashvalue)
            info.update({feature: str(",".join(str(value) for value in values))})
        return Response(json.dumps(info), status=200, mimetype="application/json")


@app.route("/getdatabase", methods=['GET'])
def get_default_database_file():
    with GbdApi(app.config['database']) as gbd_api:
        app.logger.info('Sending database to {}'.format(request.remote_addr))
        return send_file(gbd_api.get_databases()[0], 
                    as_attachment=True, 
                    attachment_filename=os.path.basename(gbd_api.get_databases()[0]),
                    mimetype='application/x-sqlite3')


def main():
    parser = argparse.ArgumentParser(description='Web- and Micro- Services to access global benchmark database.')
    parser.add_argument('-d', "--db", help='Specify database to work with', default=os.environ.get('GBD_DB'), nargs='?')
    parser.add_argument('-p', "--port", help='Specify port on which to listen', type=int)
    args = parser.parse_args()
    if not args.db:
        eprint("""No database path is given. 
A database path can be given in two ways:
-- by setting the environment variable GBD_DB
-- by giving a path via --db=[path]
A database file containing some attributes of instances used in the SAT Competitions can be obtained at http://gbd.iti.kit.edu/getdatabase
Don't forget to initialize each database with the paths to your benchmarks by using the init-command. """)
    else:
        logging_dir = "gbd-server-logs"
        logging_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), logging_dir)
        if not os.path.exists(logging_path):
            os.makedirs(logging_path)
        logging.basicConfig(filename='{}/server.log'.format(logging_path), level=logging.DEBUG)
        logging.getLogger().addHandler(default_handler)
        global app
        app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1)
        app.config['database'] = args.db
        app.static_folder = os.path.join(os.path.dirname(os.path.abspath(gbd_server.__file__)), "static")
        app.template_folder = os.path.join(os.path.dirname(os.path.abspath(gbd_server.__file__)), "templates-vue")
        app.run(host='0.0.0.0', port=args.port)


if __name__ == '__main__':
    main()
