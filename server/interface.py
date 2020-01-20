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

from os.path import join, dirname, realpath
SERVER_CONFIG_PATH = join(dirname(realpath(__file__)), "server_config")
standard_attribute = 'benchmarks'
QUERY_PATTERNS = ['clauses < 50', 'benchmarks like %battleship%', 'variables < 100']
MAX_HOURS = None  # time in hours the ZIP file remain in the cache
MAX_MINUTES = 1  # time in minutes the ZIP files remain in the cache
THRESHOLD_ZIP_SIZE = 5  # size in MB the server should zip at max
