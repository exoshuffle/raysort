import json
import math

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

# https://scipy-cookbook.readthedocs.io/items/Matplotlib_LaTeX_Examples.html
fig_width_pt = 241.14749  # Get this from LaTeX using \showthe\columnwidth
inches_per_pt = 1.0 / 72.27  # Convert pt to inches
golden_ratio = (np.sqrt(5) - 1.0) / 2.0  # Aesthetic ratio
figwidth = fig_width_pt * inches_per_pt  # width in inches
figheight = figwidth * golden_ratio  # height in inches
figsize = (figwidth, figheight)
fontsize = 7

plt.rcParams.update(
    {
        "axes.titlesize": fontsize,
        "axes.labelsize": fontsize,
        "font.size": fontsize,
        "figure.figsize": figsize,
        "figure.dpi": 150,
        "legend.fontsize": fontsize,
        "text.usetex": True,
        "xtick.labelsize": fontsize,
        "ytick.labelsize": fontsize,
    }
)

sns.set_theme(style="ticks", font_scale=1)
sns.set_palette("Set2")

# Read in json file of timestamps for map and reduce tasks
def get_json_input(fname):
    f = open(fname)
    data = json.load(f)
    map_times = []
    reduce_times = []
    max_end = 0
    start = math.inf
    for row in data:
        t = float(row["ts"] + row["dur"]) / 1000000.0  # convert to seconds
        start_t = float(row["ts"]) / 1000000.0
        if row["name"] == "map":
            map_times.append(t)
        elif row["name"] == "reduce":
            reduce_times.append(t)
        if t > max_end:
            max_end = t
        if start_t < start:
            start = start_t
    map_times.sort()
    reduce_times.sort()
    num_map_tasks = len(map_times)  # num map tasks = num mappers = num reducers.
    map_data = [
        (i * 100 / num_map_tasks, t - start, "map")
        for i, t in enumerate(map_times, start=1)
    ]
    map_data.insert(0, (0, 0.000001, "map"))
    num_reduce_tasks = len(reduce_times)  # num map tasks = num mappers = num reducers.
    reduce_data = [
        (i * 100 / num_reduce_tasks, t - start, "reduce")
        for i, t in enumerate(reduce_times, start=1)
    ]
    reduce_data.insert(0, (0, 0.000001, "reduce"))

    map_data.append((100, max_end - start, "map"))
    reduce_data.append((100, max_end - start, "reduce"))
    map_data.extend(reduce_data)

    df = pd.DataFrame(map_data, columns=["pct", "time", "Task"])

    print(df["time"].max())
    max_end = max_end - start
    return (df, max_end)


# Plot the map and reduce start times
def plot(df, end_time, figname, x="time", y="pct", hue="Task"):
    fig, ax = plt.subplots(figsize=figsize)
    g = sns.lineplot(data=df, x=x, y=y, hue=hue, ax=ax)
    plt.axvline(
        end_time,
        figure=fig,
        color="gray",
        linestyle="-",
        label="theoretical",
    )
    #    plt.fill_between(df[x].values, df[y].values, alpha=0.1)
    plt.xlim((0, int(math.ceil(end_time))))
    plt.ylim((0, 100))
    ax.yaxis.set_major_formatter(mpl.ticker.PercentFormatter(decimals=0))
    plt.xlabel("Time (s)")
    plt.ylabel("% Tasks completed")
    plt.grid(axis="y")
    filename = figname + ".pdf"
    print(filename)
    plt.savefig(filename, bbox_inches="tight")


df, end_time = get_json_input("/tmp/raysort-1650055694.215835.json")
plot(df, end_time, "map_reduce_start_times")
