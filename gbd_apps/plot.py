from gbd_tool.gbd_api import GBD
from gbd_tool import util, contexts

import os
import pandas as pd
from numpy.core.numeric import NaN

import matplotlib
matplotlib.use('GTK3Agg')
import matplotlib.pyplot as plt
import itertools

import argparse

#matplotlib.use("pgf")
#matplotlib.rcParams.update({
#    "pgf.texsystem": "pdflatex",
#    'font.family': 'serif',
#    'text.usetex': True,
#    'pgf.rcfonts': False,
#})

coolors=['#264653', '#2a9d8f', '#e9c46a', '#f4a261', '#e76f51']
coolors2=["#001219","#9b2226","#005f73","#ca6702","#0a9396","#bb3e03","#94d2bd","#ae2012","#e9d8a6","#ee9b00"]


def scatter(api: GBD, query, runtimes, timeout, groups):
    plt.rcParams.update({'font.size': 6})
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.set_aspect('equal', adjustable='box')
    plt.axline((0, 0), (1, 1), linewidth=0.5, color='grey', zorder=0)
    plt.axhline(y=timeout, xmin=0, xmax=1, linewidth=0.5, color='grey', zorder=0)
    plt.axvline(x=timeout, ymin=0, ymax=1, linewidth=0.5, color='grey', zorder=0)
    plt.xlabel(runtimes[0], fontsize=8)
    plt.ylabel(runtimes[1], fontsize=8)
    markers = itertools.cycle(plt.Line2D.markers.items())
    next(markers)
    next(markers)
    plt.rcParams['axes.prop_cycle'] = plt.cycler(color=coolors)
    if not groups:
        groups = []

    result = api.query_search(query, [], runtimes)
    dfall = pd.DataFrame(result, columns = ["hash"] + runtimes)
    for r in runtimes:
        dfall[r] = pd.to_numeric(dfall[r], errors='coerce')
        dfall.loc[(dfall[r] >= timeout) | pd.isna(dfall[r]), r] = timeout
    print(dfall)

    plots = []
    title = []
    for g in groups:
        color=next(ax._get_lines.prop_cycler)['color']
        marker=next(markers)[0]

        result = api.query_search(query + " and (" + g + ")", [], runtimes)
        df = pd.DataFrame(result, columns = ["hash"] + runtimes)
        for r in runtimes:
            df[r] = pd.to_numeric(df[r], errors='coerce')
            df.loc[(df[r] >= timeout) | pd.isna(df[r]), r] = timeout
        dfall = pd.concat([dfall, df]).drop_duplicates(keep=False)

        plots = plots + [ plt.scatter(data=df, x=runtimes[0], y=runtimes[1], c=color, marker=marker, alpha=0.7, linewidth=0.7, zorder=2) ]
        title = title + [ g ]

    plt.scatter(data=dfall, x=runtimes[0], y=runtimes[1], marker='.', alpha=0.7, linewidth=0.7, color="black", zorder=1)

    plt.legend(tuple(plots), tuple(title), scatterpoints=1, bbox_to_anchor=(0., 1.02, 1., .102), loc='lower left', ncol=5, mode="expand", borderaxespad=0.)
    plt.savefig('out.svg', transparent=True, bbox_inches='tight', pad_inches=0)
    plt.show()


def cdf(api: GBD, query, runtimes, timeout, title):
    plt.rcParams.update({'font.size': 8})
    result = api.query_search(query, [], runtimes)
    result = [[float(val) if util.is_number(val) and float(val) < float(timeout) else timeout for val in row[1:]] for row in result]
    df = pd.DataFrame(result)
    df.columns = runtimes
    df['vbs'] = df[runtimes].min(axis=1)
    print(df)

    plt.rcParams['axes.prop_cycle'] = plt.cycler(color=coolors2+coolors)

    params = {'legend.fontsize': 'small',
            'axes.labelsize': 6,
            'axes.titlesize': 6,
            'xtick.labelsize': 6,
            'ytick.labelsize': 6,
            'axes.titlepad': 10}
    plt.rcParams.update(params)

    fig = plt.figure()
    ax = fig.add_subplot(111)

    plt.xlim(0, timeout + 100)
    plt.grid(linestyle='-', linewidth=.5)
    plt.axvline(x=timeout, linestyle='dashed', color='black', linewidth=.5)
    #plt.ylim(0, len(result))

    # Build Title
    if (title is None):
        title = []
        for elem in query.split('=')[1:]:
            parts = elem.strip().split(' ')
            title = title + [parts[0].replace('_', ' ').title()]
        ax.set_title(", ".join(title), fontsize=6, variant='small-caps')
    else:
        ax.set_title(title, fontsize=6, variant='small-caps')

    df2 = pd.DataFrame(index=range(timeout+100), columns=runtimes)
    df2.fillna(0)
    for col in ['vbs'] + runtimes:
        df2[col] = [0] * (timeout + 100)
        for val in df[col]:
            if val < timeout:
                df2.loc[round(val), col] = df2[col][round(val)] + 1

        sum = 0
        for val in range(1, timeout + 100):
            df2.loc[val, col + "_"] = NaN
            if df2[col][val] != 0:
                df2.loc[val, col + "_"] = sum
            sum = sum + df2.loc[val, col]
            df2.loc[val, col] = sum
    
    markers = itertools.cycle(['o','v','^','<','>','p','P','*','h','H','8','X','d','D','s'])
    #next(markers)
    #next(markers)
    order=len(runtimes)+1
    for col in ['vbs'] + runtimes:
        color=next(ax._get_lines.prop_cycler)['color']
        ax.plot(df2[col], zorder=order, linestyle='-', linewidth=.5, color=color)
        ax.plot(df2[str(col) + "_"], label=col, zorder=order, fillstyle='none', marker=next(markers)[0], alpha=.9, markeredgewidth=.5, markersize=3, drawstyle='steps-post', color=color)
        order = order - 1
    
    plt.legend(ncol=2, loc='lower right')
    plt.savefig('out.svg', transparent=True, bbox_inches='tight', pad_inches=0)
    plt.show()

    # pgf output:
    #plt.savefig('out.pgf')

def cli_plot_scatter(api: GBD, args):
    from gbd_apps import plot
    plot.scatter(api, args.query, args.runtimes, args.tlim, args.groups)

def cli_plot_cdf(api: GBD, args):
    from gbd_apps import plot
    plot.cdf(api, args.query, args.runtimes, args.tlim, args.title)

### Argument Types for Input Sanitation in ArgParse Library
def directory_type(path):
    if not os.path.isdir(path):
        raise argparse.ArgumentTypeError('{0} is not a directory'.format(path))
    if os.access(path, os.R_OK):
        return os.path.abspath(path)
    else:
        raise argparse.ArgumentTypeError('{0} is not readable'.format(path))

def file_type(path):
    if not os.path.isfile(path):
        raise argparse.ArgumentTypeError('{0} is not a regular file'.format(path))
    if os.access(path, os.R_OK):
        return os.path.abspath(path)
    else:
        raise argparse.ArgumentTypeError('{0} is not readable'.format(path))

def column_type(s):
    pat = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]*$")
    if not pat.match(s):
        raise argparse.ArgumentTypeError('Column "{0}" does not match regular expression {1}'.format(s, pat.pattern))
    return s

def key_value_type(s):
    tup = s.split('=', 1)
    if len(tup) != 2:
        raise argparse.ArgumentTypeError('key-value type: {0} must be separated by exactly one = '.format(s))
    return (column_type(tup[0]), tup[1])

def gbd_db_type(dbstr):
    if not dbstr:
        default=os.environ.get('GBD_DB')
        if not default:
            raise argparse.ArgumentTypeError("Datasources Missing: Set GBD_DB environment variable (Get databases: http://gbd.iti.kit.edu/)")
        return default
    return dbstr

def add_query_and_hashes_arguments(parser: argparse.ArgumentParser):
    parser.add_argument('query', help='GBD Query', nargs='?')
    parser.add_argument('--hashes', help='Give Hashes as ARGS or via STDIN', nargs='*', default=[])



### Define Command-Line Interface and Map Sub-Commands to Methods
def main():
    parser = argparse.ArgumentParser(description='GBD Benchmark Database')

    parser.add_argument('-d', "--db", help='Specify database to work with', type=gbd_db_type, nargs='?', default=os.environ.get('GBD_DB'))
    parser.add_argument('-v', '--verbose', help='Print additional (or diagnostic) information to stderr', action='store_true')
    parser.add_argument('-w', '--subselect', help='Move where to subselect', action='store_true')

    parser.add_argument('-t', '--tlim', help="Time limit (sec) per instance for 'init' sub-commands (also used for score calculation in 'eval' and 'plot')", default=5000, type=int)
    parser.add_argument('-m', '--mlim', help="Memory limit (MB) per instance for 'init' sub-commands", default=2000, type=int)
    parser.add_argument('-f', '--flim', help="File size limit (MB) per instance for 'init' sub-commands which create files", default=1000, type=int)

    parser.add_argument('-s', "--separator", help="Feature separator (delimiter used in import and output", choices=[" ", ",", ";"], default=" ")
    parser.add_argument("--join-type", help="Join Type: treatment of missing values in queries", choices=["INNER", "OUTER", "LEFT"], default="LEFT")
    parser.add_argument('-c', '--context', default='cnf', choices=contexts.contexts(), 
                            help='Select context (affects selection of hash/identifier and available feature-extractors in init)')

    subparsers = parser.add_subparsers(help='Available Commands:', required=True, dest='gbd command')


    # PLOTS
    parser_plot = subparsers.add_parser('plot', help='Plot Runtimes')
    parser_plot_subparsers = parser_plot.add_subparsers(help='Select Plot', required=True, dest='plot type')

    parser_plot_scatter = parser_plot_subparsers.add_parser('scatter', help='Scatter Plot')
    add_query_and_hashes_arguments(parser_plot_scatter)
    parser_plot_scatter.add_argument('-r', '--runtimes', help='Two runtime features', nargs=2)
    parser_plot_scatter.add_argument('-g', '--groups', help='Highlight specific groups (e.g. family=cryptography)', nargs='+')
    parser_plot_scatter.set_defaults(func=cli_plot_scatter)

    parser_plot_cdf = parser_plot_subparsers.add_parser('cdf', help='CDF Plot')
    add_query_and_hashes_arguments(parser_plot_cdf)
    parser_plot_cdf.add_argument('-r', '--runtimes', help='List of runtime features', nargs='+')
    parser_plot_cdf.add_argument('--title', help='Plot Title')
    parser_plot_cdf.set_defaults(func=cli_plot_cdf)