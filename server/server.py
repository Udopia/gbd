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

import datetime
import logging
import os
import re
import threading
import random
from os.path import basename, isfile
from zipfile import ZipFile, ZipInfo

import interface
import tatsu
import util
import rendering
from flask import Flask, request, send_file, json
from flask.logging import default_handler
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from gbd_tool.gbd_api import GbdApi
from gbd_tool.hashing import gbd_hash
from gbd_tool.http_client import USER_AGENT_CLI
from tatsu import exceptions
from werkzeug.middleware.proxy_fix import ProxyFix

limiter = None
DATABASE = None
gbd_api = None
app = Flask(__name__, static_folder="static", template_folder="templates")

CACHE_PATH = 'cache'
ZIP_SEMAPHORE = threading.Semaphore(4)
ZIP_PREFIX = '_'

CSV_FILE_NAME = 'gbd'
standard_attribute = interface.standard_attribute
request_semaphore = threading.Semaphore(10)
csv_mutex = threading.Semaphore(1)  # shall stay a mutex - don't edit
zip_mutex = threading.Semaphore(1)  # shall stay a mutex - don't edit


def create_app(application, db):
    logging.basicConfig(filename='server.log', level=logging.DEBUG)
    logging.getLogger().addHandler(default_handler)
    application.wsgi_app = ProxyFix(application.wsgi_app, num_proxies=1)
    if db is None:
        fallback = os.environ.get('GBD_DB')
        if fallback is None:
            raise RuntimeError("No database given.")
        else:
            application.config['database'] = fallback
    else:
        application.config['database'] = db
    global DATABASE
    DATABASE = application.config['database']
    global limiter
    limiter = Limiter(application, key_func=get_remote_address)
    global gbd_api
    gbd_api = GbdApi(DATABASE)
    return application


@app.route("/", methods=['GET'])
def quick_search():
    request_semaphore.acquire()
    all_groups = get_group_tuples()
    request_semaphore.release()
    return rendering.render_start_page(all_groups)


def get_group_tuples():
    group_list = []
    all_groups = gbd_api.get_all_groups()
    for group in all_groups:
        group_name = group.__str__()
        is_system_table = re.match('_{2}.*', group_name)
        if not is_system_table:
            group_list.append([group, False])
    all_groups = sorted(group_list)
    return all_groups


@app.route("/results", methods=['POST'])
def quick_search_results():
    request_semaphore.acquire()
    q = request.values.get('query')
    all_groups = get_group_tuples()
    checked_groups = request.values.getlist('groups')
    groups_list = []
    for group_tuple in all_groups:
        if group_tuple[0] in checked_groups:
            groups_list.append([group_tuple[0], True])
        else:
            groups_list.append([group_tuple[0], False])
    all_groups = groups_list
    try:
        if len(checked_groups) == 0:
            if q != "":
                results = list(gbd_api.query_search(query=q, resolve=[standard_attribute], collapse=False))
            else:
                results = list(gbd_api.query_search(query=None, resolve=[standard_attribute], collapse=False))
        else:
            if q == "":
                results = list(gbd_api.query_search(query=None, resolve=checked_groups, collapse=False))
            else:
                results = list(gbd_api.query_search(query=q, resolve=checked_groups, collapse=False))
        request_semaphore.release()
        return rendering.render_result_page(
            groups=all_groups,
            results=results,
            checked_groups=checked_groups,
            query=q)
    except tatsu.exceptions.FailedParse:
        request_semaphore.release()
        return rendering.render_warning_page(
            groups=all_groups,
            checked_groups=checked_groups,
            warning_message="Whoops! Non-valid query...",
            query=q)
    except ValueError:
        request_semaphore.release()
        return rendering.render_warning_page(
            groups=all_groups,
            checked_groups=checked_groups,
            warning_message="Whoops! Something went wrong...",
            query=q)


@app.route("/exportcsv", methods=['POST'])
def get_csv_file():
    request_semaphore.acquire()
    query = request.values.get('query')
    checked_groups = request.values.getlist('groups')
    results = gbd_api.query_search(query, checked_groups, collapse=False)
    if query == "":
        query = None
    if len(results) == 0:
        request_semaphore.release()
        return rendering.render_warning_page(
            groups=get_group_tuples(),
            checked_groups=checked_groups,
            warning_message="You don't want empty CSV files, believe me.",
            query=query)
    csv_mutex.acquire()
    csv_file = create_csv_file(checked_groups, results)
    csv_mutex.release()
    app.logger.info('Sent file {} to {} at {}'.format(csv_file, request.remote_addr,
                                                      datetime.datetime.now()))
    request_semaphore.release()
    return send_file(csv_file, attachment_filename='{}.csv'.format(CSV_FILE_NAME), as_attachment=True)


def create_csv_file(checked_groups, results):
    if not os.path.isdir('{}'.format(CACHE_PATH)):
        os.makedirs('{}'.format(CACHE_PATH))
    postfix = '{}'.format(random.randint(1, 10000))
    csv = CSV_FILE_NAME + postfix
    csv_file = ''.join('{}/{}.csv'.format(CACHE_PATH, csv))
    f = open(csv_file, 'w')
    f.write(util.create_csv_string(checked_groups, results))
    f.close()
    util.delete_old_cached_files(CACHE_PATH, interface.MAX_HOURS, interface.MAX_MINUTES)
    return csv_file


@app.route("/getzip", methods=['POST'])
def get_zip_file():
    request_semaphore.acquire()
    query = request.values.get('query')
    if query == "":
        query = None
    checked_groups = request.values.getlist('groups')
    result = sorted(gbd_api.query_search(query, collapse=True))
    if len(result) == 0:
        return rendering.render_warning_page(
            groups=get_group_tuples(),
            checked_groups=checked_groups,
            warning_message="You don't want empty ZIP files, believe me.",
            query=query)
    if not os.path.isdir('{}'.format(CACHE_PATH)):
        os.makedirs('{}'.format(CACHE_PATH))
    hash_list = []
    benchmark_files = []
    for result_tuple in result:
        hash_list.append(result_tuple[0])
        benchmark_files.append(result_tuple[1])
    result_hash = gbd_hash.hash_hashlist(sorted(list(hash_list)))
    zipfile_busy = ''.join('{}/{}{}.zip'.format(CACHE_PATH, ZIP_PREFIX, result_hash))
    zipfile_ready = zipfile_busy.replace(ZIP_PREFIX, '')

    zip_mutex.acquire()
    if isfile(zipfile_ready):
        with open(zipfile_ready, 'a'):
            os.utime(zipfile_ready, None)
        zip_mutex.release()
        util.delete_old_cached_files(CACHE_PATH, interface.MAX_HOURS, interface.MAX_MINUTES)
        app.logger.info('Sent file {} to {} at {}'.format(zipfile_ready, request.remote_addr,
                                                          datetime.datetime.now()))
        request_semaphore.release()
        return send_file(zipfile_ready,
                         attachment_filename='benchmarks.zip',
                         as_attachment=True)
    elif not isfile(zipfile_busy):
        size = 0
        for file in benchmark_files:
            zf = ZipInfo.from_file(file, arcname=None)
            size += zf.file_size
        divisor = 1024 << 10
        if size / divisor < interface.THRESHOLD_ZIP_SIZE:
            thread = threading.Thread(target=create_zip, args=(zipfile_ready, benchmark_files, ZIP_PREFIX))
            thread.start()
            request_semaphore.release()
            return rendering.render_zip_reload_page(
                groups=get_group_tuples(),
                checked_groups=checked_groups,
                zip_message="ZIP is being created",
                query=query)
        else:
            zip_mutex.release()
            request_semaphore.release()
            return rendering.render_warning_page(
                groups=get_group_tuples(),
                checked_groups=checked_groups,
                warning_message="The ZIP file is too large (more than {} MB)".format(interface.THRESHOLD_ZIP_SIZE),
                query=query)
    else:
        zip_mutex.release()
        request_semaphore.release()
        return rendering.render_zip_reload_page(
            groups=get_group_tuples(),
            checked_groups=checked_groups,
            zip_message="ZIP is being created".format(interface.THRESHOLD_ZIP_SIZE),
            query=query)


def create_zip(zipfile, zip_files, prefix):
    ZIP_SEMAPHORE.acquire()
    with ZipFile(zipfile, 'w') as zf:
        for file in zip_files:
            zf.write(file, basename(file))
    zf.close()
    os.rename(zipfile, zipfile.replace(prefix, ''))
    ZIP_SEMAPHORE.release()
    zip_mutex.release()


@app.route("/query", methods=['POST'])  # query string post
def query_for_cli():
    request_semaphore.acquire()
    query = request.values.get('query')
    ua = request.headers.get('User-Agent')
    if ua == USER_AGENT_CLI:
        if query == 'None':
            hashset = gbd_api.query_search()
        else:
            try:
                hashset = gbd_api.query_search(query)
            except tatsu.exceptions.FailedParse:
                return "Illegal query"
        response = []
        for hash in hashset:
            response.append(hash)
        request_semaphore.release()
        return json.dumps(response)
    else:
        request_semaphore.release()
        return "Not allowed"


@app.route("/getdatabase", methods=['GET'])
def get_database_file():
    request_semaphore.acquire()
    global DATABASE
    app.logger.info('Sent file {} to {} at {}'.format(DATABASE, request.remote_addr,
                                                      datetime.datetime.now()))
    filename = basename(DATABASE)
    request_semaphore.release()
    return send_file(DATABASE,
                     attachment_filename=filename,
                     as_attachment=True)


if __name__ == '__main__':
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument('-d')
    args = parser.parse_args()
    database = args.d
    app = create_app(app, database)
    app.run(host='0.0.0.0')
