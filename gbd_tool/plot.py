from gbd_tool.gbd_api import GBD

import pandas as pd
import gbd_tool.util as util

from numpy.core.numeric import NaN

import matplotlib.pyplot as plt
import itertools

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