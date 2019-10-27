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
from os.path import isfile, basename
from sqlite3 import OperationalError
from zipfile import ZipFile, ZipInfo

import htmlGenerator
import interface
import tatsu
import util
from flask import Flask, render_template, request, send_file, json
from flask.logging import default_handler
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from gbd_tool.gbd_api import GbdApi
from gbd_tool.hashing import gbd_hash
from gbd_tool.http_client import USER_AGENT_CLI
from tatsu import exceptions
from werkzeug.middleware.proxy_fix import ProxyFix

logging.basicConfig(filename='server.log', level=logging.DEBUG)
logging.getLogger().addHandler(default_handler)
app = Flask(__name__, static_folder="static", template_folder="templates")
app.wsgi_app = ProxyFix(app.wsgi_app, num_proxies=1)
limiter = Limiter(app, key_func=get_remote_address)

DATABASE = os.environ.get('GBD_DB_SERVER')
if DATABASE is None:
    raise RuntimeError("No database path given. Please set path in GBD_DB environment variable!")

ZIPCACHE_PATH = 'zipcache'
ZIP_BUSY_PREFIX = '_'
MAX_HOURS_ZIP_FILES = None  # time in hours the ZIP file remain in the cache
MAX_MIN_ZIP_FILES = 1  # time in minutes the ZIP files remain in the cache
THRESHOLD_ZIP_SIZE = 5  # size in MB the server should zip at max
ZIP_SEMAPHORE = threading.Semaphore(4)

gbd_api = GbdApi(interface.SERVER_CONFIG_PATH, DATABASE)
request_semaphore = threading.Semaphore(10)
check_zips_mutex = threading.Semaphore(1)  # shall stay a mutex - don't edit


@app.route("/", methods=['GET'])
def quick_search():
    request_semaphore.acquire()
    all_groups = get_group_tuples()
    request_semaphore.release()
    return render_template('quick_search.html', groups=all_groups, is_result=False, has_query=False)


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
        results = list(gbd_api.query_search(q, checked_groups))
        request_semaphore.release()
        return render_template('quick_search_content.html', groups=all_groups, is_result=True,
                               results=results,
                               checked_groups=checked_groups, has_query=True, query=q)
    except tatsu.exceptions.FailedParse:
        request_semaphore.release()
        return render_template('quick_search_content.html', groups=all_groups,
                               is_result=True,
                               checked_groups=checked_groups,
                               contains_error=True, error_message="Whoops! Non-valid query...",
                               has_query=True, query=q)
    except ValueError:
        request_semaphore.release()
        return render_template('quick_search_content.html', groups=all_groups,
                               is_result=True,
                               checked_groups=checked_groups,
                               contains_error=True, error_message="Whoops! "
                                                                  "Something went wrong...",
                               has_query=True, query=q)


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


@app.route("/query/form", methods=['GET'])
def query_form():
    return render_template('query_form.html')


@app.route("/query", methods=['POST'])  # query string post
def query():
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


@app.route("/queryzip", methods=['POST'])
def queryzip():
    request_semaphore.acquire()
    query = request.values.get('query')
    response = htmlGenerator.generate_html_header("en")
    try:
        sorted_hash_set = sorted(gbd_api.query_search(query))
        if len(sorted_hash_set) != 0:
            if not os.path.isdir('{}'.format(ZIPCACHE_PATH)):
                os.makedirs('{}'.format(ZIPCACHE_PATH))
            result_hash = gbd_hash.hash_hashlist(sorted_hash_set)
            zipfile_busy = ''.join('{}/{}{}.zip'.format(ZIPCACHE_PATH, ZIP_BUSY_PREFIX, result_hash))
            zipfile_ready = zipfile_busy.replace(ZIP_BUSY_PREFIX, '')
            check_zips_mutex.acquire()

            if isfile(zipfile_ready):
                with open(zipfile_ready, 'a'):
                    os.utime(zipfile_ready, None)
                util.delete_old_cached_files(ZIPCACHE_PATH, MAX_HOURS_ZIP_FILES, MAX_MIN_ZIP_FILES)
                check_zips_mutex.release()
                request_semaphore.release()
                app.logger.info('Sent file {} to {} at {}'.format(zipfile_ready, request.remote_addr,
                                                                  datetime.datetime.now()))
                return send_file(zipfile_ready,
                                 attachment_filename='benchmarks.zip',
                                 as_attachment=True)
            elif not isfile(zipfile_busy):
                util.delete_old_cached_files(ZIPCACHE_PATH, MAX_HOURS_ZIP_FILES, MAX_MIN_ZIP_FILES)
                files = []
                for h in sorted_hash_set:
                    files.append(gbd_api.resolve([h], ['benchmarks'])[0].get('benchmarks'))
                size = 0
                for file in files:
                    zf = ZipInfo.from_file(file, arcname=None)
                    size += zf.file_size
                divisor = 1024 << 10
                if size / divisor < THRESHOLD_ZIP_SIZE:
                    thread = threading.Thread(target=create_zip_with_marker,
                                              args=(zipfile_busy, files, ZIP_BUSY_PREFIX))
                    thread.start()
                    check_zips_mutex.release()
                    request_semaphore.release()
                    app.logger.info('{} created zipfile {} at {}'.format(request.remote_addr, zipfile_busy,
                                                                         datetime.datetime.now()))
                    return htmlGenerator.generate_zip_busy_page(zipfile_busy, float(round(size / divisor, 2)))
                else:
                    check_zips_mutex.release()
                    response += '<hr>' \
                                '{}'.format(htmlGenerator.generate_warning("ZIP too large (size >{} MB)")
                                            .format(THRESHOLD_ZIP_SIZE))
        else:
            response += '<hr>'
            response += htmlGenerator.generate_warning("No benchmarks found")
    except exceptions.FailedParse:
        response += '<hr>'
        response += htmlGenerator.generate_warning("Non-valid query")
    except OperationalError:
        response += '<hr>'
        response += htmlGenerator.generate_warning("Group not found")
    request_semaphore.release()
    return response


@app.route("/resolve/form", methods=['GET'])
def resolve_form():
    return render_template('resolve_form.html')


@app.route("/resolve", methods=['POST'])
def resolve():
    request_semaphore.acquire()
    ua = request.headers.get('User-Agent')
    if ua == USER_AGENT_CLI:
        result = handle_cli_resolve_request(request)
    else:
        return "Not allowed"
    request_semaphore.release()
    return result


def handle_cli_resolve_request(req):
    hashed = json.loads(req.values.get("hashes"))
    groups = json.loads(req.values.get("group"))
    shall_collapse = req.values.get("collapse") == "True"
    pattern = req.values.get("pattern")

    entries = []
    try:
        if pattern != 'None':
            dict_list = gbd_api.resolve(hashed, groups,
                                        collapse=shall_collapse,
                                        pattern=pattern)
        else:
            dict_list = gbd_api.resolve(hashed, groups, collapse=shall_collapse)
        for d in dict_list:
            entries.append(d)
        return json.dumps(entries)
    except OperationalError:
        return json.dumps("Group not found")
    except IndexError:
        return json.dumps("Hash not found in our DATABASE")


@app.route("/groups/reflect", methods=['GET'])
def reflect_group():
    request_semaphore.acquire()
    try:
        group = request.args.get('group')
        if not group.startswith("__"):
            info = gbd_api.get_group_info(group)
            list = ["Name: {}".format(info.get('name')),
                    "Type: {}".format(info.get('type')),
                    "Unique: {}".format(info.get('unique')),
                    "Default: {}".format(info.get('default')),
                    "Size: {}".format(info.get('entries'))]
            request_semaphore.release()
            return list.__str__()
        else:
            request_semaphore.release()
            return "__ is reserved for system tables"
    except IndexError:
        request_semaphore.release()
        return "Group not found"


@app.route("/zips/busy", methods=['GET'])
def get_zip():
    request_semaphore.acquire()
    zipfile = request.args.get('file')
    if isfile(zipfile.replace(ZIP_BUSY_PREFIX, '')):
        request_semaphore.release()
        app.logger.info('Sent file {} to {} at {}'.format(zipfile.replace(ZIP_BUSY_PREFIX, ''), request.remote_addr,
                                                          datetime.datetime.now()))
        return send_file(zipfile.replace(ZIP_BUSY_PREFIX, ''), attachment_filename='benchmarks.zip', as_attachment=True)
    elif not isfile('_{}'.format(zipfile)):
        request_semaphore.release()
        return htmlGenerator.generate_zip_busy_page(zipfile, 0)


def create_zip_with_marker(zipfile, files, prefix):
    ZIP_SEMAPHORE.acquire()
    with ZipFile(zipfile, 'w') as zf:
        for file in files:
            zf.write(file, basename(file))
    zf.close()
    os.rename(zipfile, zipfile.replace(prefix, ''))
    ZIP_SEMAPHORE.release()
