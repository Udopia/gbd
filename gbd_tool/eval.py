from gbd_tool.gbd_api import GbdApi
import gbd_tool.util as util

from itertools import combinations
from operator import itemgetter

def par2(api: GbdApi, query, runtimes, timeout, divisor):
    for name in runtimes:
        times = api.query_search(query, [], [name])
        div = len(times) if divisor is None else divisor
        par2 = sum(float(time[1]) if util.is_number(time[1]) and float(time[1]) < timeout else 2*timeout for time in times) / div
        solved = sum(1 if util.is_number(time[1]) and float(time[1]) < timeout else 0 for time in times)
        print(str(round(par2, 2)) + " " + str(solved) + "/" + str(div) + " " + name)
    times = api.query_search(query, [], runtimes)
    div = len(times) if divisor is None else divisor
    vbs_par2 = sum([min(float(val) if util.is_number(val) and float(val) < timeout else 2*timeout for val in row[1:]) for row in times]) / div
    solved = sum(1 if t < timeout else 0 for t in [min(float(val) if util.is_number(val) else 2*timeout for val in row[1:]) for row in times])
    print(str(round(vbs_par2, 2)) + " " + str(solved) + "/" + str(div) + " VBS")

def vbs(api: GbdApi, query, runtimes, timeout, separator):
    resultset = api.calculate_vbs(query, runtimes, timeout)
    for result in resultset:
        print(separator.join([(str(item or '')) for item in result]))

def greedy_comb(api: GbdApi, query, runtimes, timeout, size):
    result = api.query_search(query, [], runtimes)
    result = [[float(val) if util.is_number(val) and float(val) < float(timeout) else 2*timeout for val in row] for row in result]
    runtimes.insert(0, "dummy")
    for comb in combinations(range(1, len(runtimes)), size):
        comb_par2 = sum([min(itemgetter(*comb)(row)) for row in result]) / len(result)
        print(str(itemgetter(*comb)(runtimes)) + ": " + str(comb_par2))