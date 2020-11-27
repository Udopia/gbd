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

import logging
import os
import re
import argparse

from gbd_tool.util import eprint
from os.path import basename

import gbd_server

import flask
from flask import Flask, request, send_file, json, Response
from flask import render_template
from logging.handlers import TimedRotatingFileHandler
from gbd_tool.gbd_api import GbdApi
from gbd_tool.error import *
from werkzeug.middleware.proxy_fix import ProxyFix

app = Flask(__name__)


# Returns main index page
@app.route("/", methods=['GET'])
def quick_search():
    with GbdApi(app.config['database']) as gbd_api:
        pass
    return render_template('index.html')


# Expects POST form with a query as text input and selected features as checkbox inputs,
# returns result as a serialized JSON object
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
            rows = list(gbd_api.query_search(query, [], features))
            features.insert(0, "GBDhash")
            result = list(dict((features[index], row[index]) for index in range(0, len(features))) for row in rows)
            app.logger.info("Answer normal query request from {}".format(request.remote_addr))
            return Response(json.dumps(result), status=200, mimetype="application/json")
        except GbdApiParsingFailed:
            message = "Could not parse your query. Please check the syntax"
        except GbdApiDatabaseError:
            message = "Operational error of database. You probably defined a non existent feature"
        except GbdApiError:
            message = "Unknown error. Please contact our team so that we can reproduce this bug"
        app.logger.error("While handling query search: {}, IP: {}".format(message, request.remote_addr))
        return Response(message, status=400, mimetype="text/plain")


# Expects POST form with a query as text input and selected features as checkbox inputs,
# sends csv version of the result as a file
@app.route("/exportcsv", methods=['POST'])
def get_csv_file():
    with GbdApi(app.config['database']) as gbd_api:
        query = request.form.get('query')
        selected_features = list(filter(lambda x: x != '', request.form.get('selected_features').split(',')))
        if not len(selected_features):
            selected_features.append("filename")
        try:
            results = gbd_api.query_search(query, [], selected_features)
        except GbdApiFeatureNotFound:
            app.logger.error("While handling CSV file request: Feature not found, IP: {}".format(request.remote_addr))
            return Response("Feature not found", status=400, mimetype="text/plain")
        headers = ["hash"] + selected_features
        content = "\n".join([" ".join([str(entry) for entry in result]) for result in results])
        file_name = "query_result.csv"
        app.logger.info("Sending CSV file to {}".format(request.remote_addr))
        return Response(" ".join(headers) + "\n" + content, mimetype='text/csv',
                        headers={"Content-Disposition": "attachment; filename=\"{}\"".format(file_name),
                                 "filename": "{}".format(file_name)})


# Generates a list of URLs. Given query (text field of POST form) is executed and the hashes of the result are resolved
# against the filename feature. Every filename is associated with a URL to enable flexible downloading of these files
@app.route("/getinstances", methods=['POST'])
def get_url_file():
    with GbdApi(app.config['database']) as gbd_api:
        query = request.form.get('query')
        try:
            result = gbd_api.query_search(query, [], ["filename"])
        except GbdApiFeatureNotFound:
            app.logger.critical(
                "While handling URL file request: 'filename' feature not found, IP: {}".format(request.remote_addr))
            return Response("This should not happen", status=500, mimetype="text/plain")
        content = "\n".join(
            [flask.url_for("get_file", hashvalue=row[0], filename=row[1], _external=True) for row in result])
        file_name = "query_result.uri"
        app.logger.info("Sending CSV file to {}".format(request.remote_addr))
        return Response(content, mimetype='text/uri-list',
                        headers={"Content-Disposition": "attachment; filename=\"{}\"".format(file_name),
                                 "filename": "{}".format(file_name)})


# Return all basenames of the databases which the server was initialized with
@app.route("/listdatabases", methods=["GET"])
def list_databases():
    with GbdApi(app.config['database']) as gbd_api:
        app.logger.info("List all databases for IP {}".format(request.remote_addr))
        return Response(json.dumps(list(map(basename, gbd_api.get_databases()))), status=200,
                        mimetype="application/json")


# Send a desired database file, if it exists
@app.route('/getdatabase', defaults={'database': None})
@app.route('/getdatabase/<database>')
def get_database_file(database):
    with GbdApi(app.config['database']) as gbd_api:
        if database is None:
            app.logger.info("Send default database file to IP {}".format(request.remote_addr))
            return send_file(gbd_api.get_databases()[0],
                             as_attachment=True,
                             attachment_filename=os.path.basename(gbd_api.get_databases()[0]),
                             mimetype='application/x-sqlite3')
        elif database not in list(map(basename, gbd_api.get_databases())):
            app.logger.warning(
                "Device with IP {} requested database '{}' which could not be found".format(request.remote_addr,
                                                                                            database))
            return Response("Database does not exist in the running instance of GBD server", status=404,
                            mimetype="text/plain")
        else:
            app.logger.info("Sending database file '{}' to IP {}".format(database, request.remote_addr))
            return send_file(list(filter(lambda x: basename(x) == database, gbd_api.get_databases()))[0],
                             as_attachment=True,
                             attachment_filename=database,
                             mimetype='application/x-sqlite3')


# Get either all cumulative features or features in a specified database (argument is basename of database file)
@app.route('/listfeatures', defaults={'database': None})
@app.route('/listfeatures/<database>')
def list_features(database):
    with GbdApi(app.config['database']) as gbd_api:
        if database is None:
            available_features = sorted(gbd_api.get_features())
            available_features.remove("local")
            app.logger.info("List all features for IP {}".format(request.remote_addr))
            return Response(json.dumps(available_features), status=200, mimetype="application/json")
        elif database not in list(map(basename, gbd_api.get_databases())):
            app.logger.warning(
                "Device with IP {} requested features of database '{}'. Database was not found".format(
                    request.remote_addr,
                    database))
            return Response("Database does not exist in the running instance of GBD server", status=404,
                            mimetype="text/plain")
        else:
            target_database = list(filter(lambda x: basename(x) == database, gbd_api.get_databases()))[0]
            app.logger.info("List all features of database '{}' for IP {}".format(database, request.remote_addr))
            return Response(json.dumps(gbd_api.get_features(target_database)), status=200, mimetype="application/json")


# Get meta data and default value of feature
@app.route('/features/info/<feature>/<database>')
def get_feature_info(feature, database):
    with GbdApi(app.config['database']) as gbd_api:
        try:
            if database in list(map(basename, gbd_api.get_databases())):
                records = gbd_api.get_feature_info(feature, list(
                    filter(lambda x: basename(x) == database, gbd_api.get_databases()))[0])
            else:
                app.logger.warning(
                    "Device with IP {} requested info of feature in database '{}'. Database was not found".format(
                        request.remote_addr,
                        database))
                return Response("Database does not exist in the running instance of GBD server", status=404,
                                mimetype="text/plain")
            return Response(json.dumps(records), status=200, mimetype="application/json")
        except GbdApiFeatureNotFound:
            app.logger.error(
                "While requesting feature info: Feature '{}' not found, IP: {}".format(feature,
                                                                                       request.remote_addr))
            return Response("Feature not found", status=500, mimetype="text/plain")


# Resolves a hashvalue against a attribute and returns the result values
@app.route('/attribute/<feature>/<hashvalue>')
def get_attribute(feature, hashvalue):
    with GbdApi(app.config['database']) as gbd_api:
        try:
            values = gbd_api.search(feature, hashvalue)
            if len(values) == 0:
                app.logger.warning(
                    "Device with IP {} issued questionable resolve '{}' -> '{}'".format(request.remote_addr, hashvalue,
                                                                                        feature))
                return Response("No feature associated with this hash", status=404, mimetype="text/plain")
            app.logger.info(
                "Resolved '{}' against feature '{}' for IP {}".format(hashvalue, feature, request.remote_addr))
            return str(",".join(str(value) for value in values))
        except GbdApiFeatureNotFound:
            app.logger.error(
                "While handling resolving hash value: Feature '{}' not found, IP: {}".format(feature,
                                                                                             request.remote_addr))
            return Response("Feature not found", status=500, mimetype="text/plain")


# Allows users to set tags in the tags table
@app.route('/tag/<hash>/<name>', defaults={'value': 'true'})
@app.route('/tag/<hash>/<name>/<value>')
def set_tag(hash, name, value):
    pat = re.compile(r"^[a-zA-Z0-9_]*$")
    if not (pat.match(hash) and pat.match(name) and pat.match(value)):
        app.logger.warning(
            "IP {} violated restriction to alpha-numeric characters and underline".format(request.remote_addr))
        return Response("Input violates restriction to alpha-numeric characters and underline", status=406,
                        mimetype="text/plain")
    with GbdApi(app.config['database']) as gbd_api:
        try:
            if gbd_api.get_feature_size("tags") > 10 * gbd_api.get_feature_size("local"):
                message = "Too many tags, cleanup required"
                app.logger.warning(message)
                return Response(message, status=503, mimetype="text/plain")
            else:
                gbd_api.set_tag(name, value, [hash])
                app.logger.info("{} set tag {}={} for {}".format(request.remote_addr, name, value, hash))
                return Response("Successfully set tag {}={} for {}".format(name, value, hash), status=201,
                                mimetype="text/plain")
        except GbdApiFeatureNotFound:
            app.logger.error(
                "While handling setting tag: One of the features defined in set_tag not found, IP: {}".format(
                    request.remote_addr))
            return Response(message, status=406, mimetype="text/plain")


# Get all attributes associated with the hashvalue (resolving against all features)
@app.route('/info/<hashvalue>')
def get_all_attributes(hashvalue):
    with GbdApi(app.config['database']) as gbd_api:
        features = gbd_api.get_features()
        info = dict([])
        for feature in features:
            try:
                values = gbd_api.search(feature, hashvalue)
            except GbdApiFeatureNotFound:
                app.logger.critical("GbdApi returned feature GbdApi.search could not find. This should not happen!")
            info.update({feature: str(",".join(str(value) for value in values))})
        app.logger.info("List all attributes of hashvalue {} for IP {}".format(hashvalue, request.remote_addr))
        return Response(json.dumps(info), status=200, mimetype="application/json")


# Find the file corresponding to the hashvalue and send it to the client
@app.route('/file/<hashvalue>', defaults={'filename': None})
@app.route('/file/<hashvalue>/<filename>')
def get_file(hashvalue, filename):
    with GbdApi(app.config['database']) as gbd_api:
        try:
            values = gbd_api.search("local", hashvalue)
        except GbdApiFeatureNotFound:
            app.logger.critical("GbdApi could not find 'local' feature")
        if len(values) == 0:
            app.logger.warning(
                "{} requested file for hash '{}', which was not found".format(request.remote_addr, hashvalue))
            return Response("No according file found in our database", status=404, mimetype="text/plain")
        try:
            path = values.pop()
            app.logger.info("Sending file for hashvalue '{}' to {}".format(hashvalue, request.remote_addr))
            return send_file(path, as_attachment=True, attachment_filename=os.path.basename(path))
        except FileNotFoundError:
            app.logger.critial("Files for hashvalues are not accessible")
            return Response("Files temporarily not accessible", status=404, mimetype="text/plain")


# Main method which configures Flask app at startup
def main():
    parser = argparse.ArgumentParser(description='Web- and Micro- Services to access global benchmark database.')
    parser.add_argument('-d', "--db", help='Specify database to work with', default=os.environ.get('GBD_DB'), nargs='?')
    parser.add_argument('-p', "--port", help='Specify port on which to listen', type=int)
    parser.add_argument('-b', "--backup",
                        help='Specify the amount of kept logging files while log rotation (every midnight)',
                        default=10,
                        type=int)
    args = parser.parse_args()
    if not args.db:
        eprint("""No database path is given. 
A database path can be given in two ways:
-- by setting the environment variable GBD_DB
-- by giving a path via --db=[path]
A database file containing some attributes of instances used in the SAT Competitions can be obtained at http://gbd.iti.kit.edu/getdatabase
Don't forget to initialize each database with the paths to your benchmarks by using the init-command. """)
    else:
        logging_dir = os.environ.get('GBD_LOGGING_DIR')
        if (logging_dir == '') or (logging_dir is None):
            print("""  --------------
  WARNING: Created logging file in execution path.
  If you wish, specify directory for the logging file with 'export GBD_LOGGING_DIR=<your_directory>' and restart
  --------------""")
            logging_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gbd-server-logs")
        if not os.path.exists(logging_dir):
            os.makedirs(logging_dir)
        formatter = logging.Formatter(
            fmt='\n'.join([
                '[%(name)s] %(asctime)s.%(msecs)d',
                '\t%(pathname)s [line: %(lineno)d]',
                '\t%(processName)s[%(process)d] => %(threadName)s[%(thread)d] => %(module)s.%(filename)s:%(funcName)s()',
                '\t%(levelname)s: %(message)s\n'
            ]),
            datefmt='%Y-%m-%d %H:%M:%S')
        logging_file = '{}/{}'.format(logging_dir, "gbd-server-log")
        logging.getLogger().setLevel(logging.DEBUG)
        # Add sys.stdout for real time logging output
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.INFO)
        logging.getLogger().addHandler(console_handler)
        # Add handler to write in rotating logging files
        file_handler = TimedRotatingFileHandler(logging_file, when="midnight", backupCount=args.backup)
        file_handler.setFormatter(formatter)
        if os.environ.get('FLASK_ENV') == 'production':
            file_handler.setLevel(logging.WARNING)
        else:
            file_handler.setLevel(logging.DEBUG)
        logging.getLogger().addHandler(file_handler)
        global app
        app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1)
        app.config['database'] = args.db
        app.static_folder = os.path.join(os.path.dirname(os.path.abspath(gbd_server.__file__)), "static")
        app.template_folder = os.path.join(os.path.dirname(os.path.abspath(gbd_server.__file__)), "templates-vue")
        app.run(host='0.0.0.0', port=args.port)


if __name__ == '__main__':
    main()
