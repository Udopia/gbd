#!/usr/bin/python3
# -*- coding: utf-8 -*-

# GBD Benchmark Database (GBD)
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
import argparse

import flask
from flask import Flask, request, send_file, json, Response
from flask import render_template

from logging.handlers import TimedRotatingFileHandler

from werkzeug.middleware.proxy_fix import ProxyFix

from gbd_tool import util
from gbd_tool.gbd_api import GBD, GBDException

import gbd_server

app = Flask(__name__)


# Returns main index page
@app.route("/", methods=['GET'])
def quick_search():
    return render_template('index.html')


# Expects POST form with a query as text input and selected features as checkbox inputs,
# returns result as a serialized JSON object
@app.route("/results", methods=['POST'])
def quick_search_results():
    query = request.form.get('query')
    selected_features = list(filter(lambda x: x != '', request.form.get('selected_features').split(',')))
    features = sorted(list(set(app.config['features']['all']) & set(selected_features or ['filename']) - {'local'}))
    app.logger.info("Received query '{}' from {}".format(query, request.remote_addr))
    with GBD(app.config['database'], verbose=app.config['verbose']) as gbd_api:
        try:
            rows = gbd_api.query_search(query, [], features)
            features.insert(0, "GBDhash")
            result = list(dict((features[index], row[index]) for index in range(0, len(features))) for row in rows)
            return Response(json.dumps(result), status=200, mimetype="application/json")
        except GBDException as err:
            app.logger.error("While handling query search: {}, IP: {}".format(err.message, request.remote_addr))
            return Response(err.message, status=400, mimetype="text/plain")


# Expects POST form with a query as text input and selected features as checkbox inputs,
# sends csv version of the result as a file
@app.route("/exportcsv/", methods=['POST', 'GET'])
@app.route("/exportcsv/<context>", methods=['POST', 'GET'])
def get_csv_file(context='cnf'):
    with GBD(app.config['database'], verbose=app.config['verbose']) as gbd_api:
        query = None
        if "query" in request.form:
            query = request.form.get('query')
        elif len(request.args) > 0:
            query = " and ".join(["{}={}".format(key, value) for (key, value) in request.args.items()])
        selected_features = None
        if "selected_features" in request.form:
            selected_features = list(filter(lambda x: x != util.prepend_context("hash", context) and x != '', request.form.get('selected_features').split(',')))
        else:
            selected_features = list(filter(lambda x: x != util.prepend_context("hash", context) and util.context_from_name(x) == context, gbd_api.get_features()))
        try:
            results = gbd_api.query_search(query, [], selected_features, group_by=util.prepend_context("hash", context))
        except GBDException as err:
            app.logger.error("While handling data request: {}, IP: {}".format(err.message, request.remote_addr))
            return Response(err.message, status=400, mimetype="text/plain")
        headers = [ util.prepend_context("hash", context) ] + selected_features
        content = "\n".join([" ".join([str(entry) for entry in result]) for result in results])
        file_name = "query_result.csv"
        app.logger.info("Sending CSV file to {}".format(request.remote_addr))
        return Response(" ".join(headers) + "\n" + content, mimetype='text/csv',
                        headers={"Content-Disposition": "attachment; filename=\"{}\"".format(file_name),
                                 "filename": file_name})


# Generates a list of URLs. Given query (text field of POST form) is executed and the hashes of the result are resolved
# against the filename feature. Every filename is associated with a URL to enable flexible downloading of these files
@app.route("/getinstances/", methods=['POST', 'GET'])
@app.route("/getinstances/<context>", methods=['POST', 'GET'])
def get_url_file(context='cnf'):
    with GBD(app.config['database'], verbose=app.config['verbose']) as gbd_api:
        query = None
        if "query" in request.form:
            query = request.form.get('query')
        elif len(request.args) > 0:
            query = " and ".join(["{}={}".format(key, value) for (key, value) in request.args.items()])
        try:
            result = gbd_api.query_search(query, [], [util.prepend_context("filename", context)], group_by=util.prepend_context("hash", context))
        except GBDException as err:
            app.logger.error("While handling instance request: {}, IP: {}".format(err.message, request.remote_addr))
            return Response(err.message, status=500, mimetype="text/plain")
        content = "\n".join(
            [flask.url_for("get_file", hashvalue=row[0], context=context, _external=True) for row in result])
        file_name = "query_result.uri"
        app.logger.info("Sending CSV file to {}".format(request.remote_addr))
        return Response(content, mimetype='text/uri-list',
                        headers={"Content-Disposition": "attachment; filename=\"{}\"".format(file_name),
                                 "filename": "{}".format(file_name)})


# Return list of databases
@app.route("/listdatabases", methods=["GET"])
def list_databases():
    app.logger.info("Listing all databases for IP {}".format(request.remote_addr))
    return Response(json.dumps(app.config['dbnames']), status=200, mimetype="application/json")


# Send database file
@app.route('/getdatabase/')
@app.route('/getdatabase/<database>')
def get_database_file(database=None):
    dbname=database if database and database in app.config['dbnames'] else app.config['dbnames'][0]
    dbpath=app.config['dbpaths'][dbname]
    dbfile=os.path.basename(dbpath)
    app.logger.info("Sending database '{}' to IP {}".format(dbfile, request.remote_addr))
    return send_file(dbpath, as_attachment=True, attachment_filename=dbfile, mimetype='application/x-sqlite3')


# Get all features or features in a specified database
@app.route('/listfeatures/')
@app.route('/listfeatures/<database>')
def list_features(database=None):
    app.logger.info("Listing features of database '{}' for IP {}".format(database or " ", request.remote_addr))
    if database and database in app.config['dbnames']:
        return Response(json.dumps(app.config['features'][database]), status=200, mimetype="application/json")
    else:
        return Response(json.dumps(app.config['features']['all']), status=200, mimetype="application/json")
        

# Resolves a hashvalue against a attribute and returns the result values
@app.route('/attribute/<feature>/<hashvalue>')
def get_attribute(feature, hashvalue):
    app.logger.info("Resolving '{}' with feature '{}' for IP {}".format(hashvalue, feature, request.remote_addr))
    with GBD(app.config['database'], verbose=app.config['verbose']) as gbd_api:
        try:
            records = gbd_api.query_search(hashes=[hashvalue], resolve=[feature])
            if len(records) == 0:
                return Response("No feature associated with this hash", status=404, mimetype="text/plain")                
            return records[0][1]
        except GBDException as err:
            app.logger.error("While handling feature request: {}, IP: {}".format(err.message, request.remote_addr))
            return Response(err.message, status=500, mimetype="text/plain")


# Get all attributes associated with the hashvalue (resolving against all features)
@app.route('/info/<hashvalue>/')
@app.route('/info/<hashvalue>/<context>')
def get_all_attributes(hashvalue, context='cnf'):
    app.logger.info("Listing all attributes of hashvalue {} for IP {}".format(hashvalue, request.remote_addr))
    with GBD(app.config['database'], verbose=app.config['verbose']) as gbd_api:
        features = app.config['features']['all']
        try:
            records = gbd_api.query_search(hashes=[hashvalue], resolve=features, group_by=util.prepend_context("hash", context))
            return Response(json.dumps(zip(features, records)), status=200, mimetype="application/json")
        except GBDException as err:
            app.logger.error("While handling feature request: {}, IP: {}".format(err.message, request.remote_addr))
            return Response(err.message, status=500, mimetype="text/plain")


# Find the file corresponding to the hashvalue and send it to the client
@app.route('/file/<hashvalue>/')
@app.route('/file/<hashvalue>/<context>')
def get_file(hashvalue, context='cnf'):
    with GBD(app.config['database'], verbose=app.config['verbose'])as gbd_api:
        local = util.prepend_context("local", context)
        filename = util.prepend_context("filename", context)
        records = gbd_api.query_search(hashes=[hashvalue], resolve=[local, filename], collapse="MIN")
        app.logger.info(str(records))
        if len(records) == 0:
            app.logger.warning("{} requested file for hash '{}' not found".format(request.remote_addr, hashvalue))
            return Response("Hash not found", status=404, mimetype="text/plain")
        try:
            app.logger.info("Sending file for hashvalue '{}' to {}".format(hashvalue, request.remote_addr))
            return send_file(records[0][1], as_attachment=True, attachment_filename=records[0][2])
        except FileNotFoundError:
            app.logger.critial("Files for hashvalues are not accessible")
            return Response("Files temporarily not accessible", status=404, mimetype="text/plain")


# Main method which configures Flask app at startup
def main():
    pwd=os.path.dirname(os.path.abspath(gbd_server.__file__))
    parser = argparse.ArgumentParser(description='Web- and Micro- Services to access global benchmark database.')
    parser.add_argument('-d', "--db", help='Specify database to work with', default=os.environ.get('GBD_DB'), nargs='?')
    parser.add_argument('-l', "--logdir", help='Specify logging dir', default=os.environ.get('GBD_LOGGING_DIR') or pwd, nargs='?')
    parser.add_argument('-p', "--port", help='Specify port on which to listen', type=int)
    parser.add_argument('-v', "--verbose", help='Verbose Mode', action='store_true')
    args = parser.parse_args()
    formatter = logging.Formatter(fmt='[%(asctime)s, %(name)s, %(levelname)s] %(module)s.%(filename)s.%(funcName)s():%(lineno)d\n%(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    logging.getLogger().setLevel(logging.DEBUG)
    # Add sys.stdout to logging output
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    logging.getLogger().addHandler(console_handler)
    # Add handler to write in rotating logging files
    file_handler = TimedRotatingFileHandler(os.path.join(args.logdir, "gbd-server-log"), when="midnight", backupCount=10)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.WARNING)
    logging.getLogger().addHandler(file_handler)
    global app
    if not args.db:
        app.logger.error("No Database Given")
        exit(1)
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1)
    app.config['database'] = args.db
    app.config['verbose'] = args.verbose
    try:
        with GBD(app.config['database'], verbose=app.config['verbose']) as gbd:
            app.config['dbnames'] = gbd.get_databases()
            if "main" in app.config['dbnames']:
                app.config['dbnames'].remove("main")
            app.config['features'] = { 'all': gbd.get_features() }
            app.config['dbpaths'] = dict()
            for db in app.config['dbnames']:
                if db != 'main':
                    app.config['features'][db] = gbd.get_features(dbname=db)
                    if "local" in app.config['features'][db]:
                        app.config['features'][db].remove("local")
                    app.config['dbpaths'][db] = gbd.get_database_path(db)
    except Exception as e:
        app.logger.error(str(e))
        exit(1)
    app.static_folder = os.path.join(pwd, "static")
    app.template_folder = os.path.join(pwd, "templates-vue")
    #app.run(host='0.0.0.0', port=args.port)
    from waitress import serve
    serve(app, host="0.0.0.0", port=5000)


if __name__ == '__main__':
    main()
