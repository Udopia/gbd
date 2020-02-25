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
import os
from zipfile import ZipInfo

import gbd_server.interface as interface


def delete_old_cached_files(directory, max_hours, max_minutes):
    """
        Delete all files in list if they are older than max_hours or max_minutes
    """
    if max_hours is not None:
        if max_hours < 0 or max_hours >= 24:
            return -1
    elif max_minutes is not None:
        if max_minutes < 0 or max_minutes >= 60:
            return -1
    files = os.listdir(directory)
    for file in files:
        path = "{}/{}".format(directory, file)
        zf = ZipInfo.from_file(path, arcname=None)
        accessed_on_datetime = datetime.datetime(*zf.date_time)
        current_datetime = datetime.datetime.now()
        diff_year = current_datetime.year - accessed_on_datetime.year
        diff_month = current_datetime.month - accessed_on_datetime.month
        timedelta = current_datetime - accessed_on_datetime
        diff_hour = current_datetime.hour - accessed_on_datetime.hour
        diff_minute = current_datetime.minute - accessed_on_datetime.minute
        if diff_year != 0 or diff_month != 0 or timedelta.days != 0:
            os.remove(path)
        elif (max_hours is not None) and (diff_hour >= max_hours) and (max_minutes is None):
            os.remove(path)
        elif (max_minutes is not None) and (diff_minute >= max_minutes) and (max_hours is None):
            os.remove(path)
        elif (max_hours is not None) and (max_minutes is not None) \
                and diff_hour >= max_hours and diff_minute >= max_minutes:
            os.remove(path)
    return 0


def create_csv_string(headers, contents):
    csv_string = "Hash "
    if len(headers) == 0:
        headers = [interface.standard_attribute]
    header_string = ' '.join(str(header) for header in headers)
    header_string += "\n"
    csv_string += header_string
    for content in contents:
        content_string = ' '.join(str(e) for e in content)
        content_string += "\n"
        csv_string += content_string
    return csv_string


def generate_query_patterns():
    return interface.QUERY_PATTERNS
