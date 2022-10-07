import json
import math

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

# https://scipy-cookbook.readthedocs.io/items/Matplotlib_LaTeX_Examples.html
# Get fig_width_pt from LaTeX using \the\columnwidth
fig_width_pt = 240.94499  # acmart-SIGPLAN
inches_per_pt = 1.0 / 72.27  # Convert pt to inches
golden_ratio = (np.sqrt(5) - 1.0) / 2.0  # Aesthetic ratio
figwidth = fig_width_pt * inches_per_pt  # width in inches
figheight = figwidth * golden_ratio  # height in inches
figsize = (figwidth, figheight)
fontsize = 9

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
def get_json_input(fnames, labels):
    if len(fnames) != len(labels):
        print("Need one label per input file")
        return None
    full_data = []
    overall_end = 0
    for i, fname in enumerate(fnames):
        label = labels[i]
        f = open(fname)
        data = json.load(f)
        reduce_times = []
        max_end = 0
        start = math.inf
        for row in data:
            t = float(row["ts"] + row["dur"]) / 1000000.0  # convert to seconds
            start_t = float(row["ts"]) / 1000000.0
            if row["name"] == "reduce":
                reduce_times.append(t)
            if t > max_end:
                max_end = t
            if start_t < start:
                start = start_t
        reduce_times.sort()
        num_reduce_tasks = len(
            reduce_times
        )  # num map tasks = num mappers = num reducers.
        reduce_data = [
            (i * 100 / num_reduce_tasks, t - start, label)
            for i, t in enumerate(reduce_times, start=1)
        ]
        reduce_data.insert(0, (0, 0.000001, label))

        reduce_data.append((100, max_end - start, label))
        full_data.extend(reduce_data)

        if (max_end - start) > overall_end:
            overall_end = max_end - start

    df = pd.DataFrame(full_data, columns=["pct", "time", "Mode"])

    print(df["time"].max())
    return (df, overall_end)


# Plot the map and reduce start times
def plot(df, end_time, figname, x="time", y="pct", hue="Mode"):
    fig, ax = plt.subplots(figsize=figsize)
    ax.set_ylabel("\% reducer outputs available", fontsize=11)
    g = sns.lineplot(data=df, x=x, y=y, hue=hue, ax=ax)
    plt.axvline(
        end_time,
        figure=fig,
        color="gray",
        linestyle="dotted",
        linewidth=3,
        label="Unpipelined\nfull shuffle",
    )
    #    plt.fill_between(df[x].values, df[y].values, alpha=0.1)
    plt.xlim((0, int(math.ceil(end_time)) + 10))
    plt.ylim((0, 100))
    ax.yaxis.set_major_formatter(mpl.ticker.PercentFormatter(decimals=0))
    plt.grid(axis="y")
    plt.xlabel("Time (s)")
    plt.legend()
    plt.tight_layout()
    filename = figname + ".pdf"
    print(filename)
    plt.savefig(filename, bbox_inches="tight")


# df, end_time = get_json_input(["/tmp/raysort-1649983857.9514349.json", "/tmp/raysort-1649984648.0972028.json", "/tmp/raysort-1649985238.1805532.json"], ["no streaming", "partial streaming", "full streaming"])
# df, end_time = get_json_input(["/tmp/raysort-1649999878.7873666.json", "/tmp/raysort-1649999446.88543.json", "/tmp/raysort-1650000048.7228544.json"], ["No streaming", "Partial streaming", "Full streaming"])
# df, end_time = get_json_input(["/tmp/raysort-1650002665.0105438.json", "/tmp/raysort-1650002134.729337.json", "/tmp/raysort-1650002428.8224025.json"], ["No streaming", "Partial streaming", "Full streaming"])
# plot(df, 47.67, "reduce_completion_times") # 47.67 was simple shuffle runtime
# 100 GB data size, 0.5GB partitions
# df, end_time = get_json_input(["/tmp/raysort-1650055694.215835.json", "/tmp/raysort-1650056036.958032.json", "/tmp/raysort-1650056193.9092405.json"], ["No streaming", "Partial streaming", "Full streaming"])
# 200 GB data size, 1GB partitions
# df, end_time = get_json_input(["/tmp/raysort-1650064050.5121264.json", "/tmp/raysort-1650063270.6768267.json", "/tmp/raysort-1650063682.941706.json", "/tmp/raysort-1650064346.3131113.json", ""], ["Full shuffle", "Partial shuffle (shuffle 1/5)", "Partial shuffle (shuffle 1/3)", "Full shuffle, no streaming", "Partial shuffle (shuffle 1/4)"])
df, end_time = get_json_input(
    ["/tmp/raysort-1650066741.546453.json", "/tmp/raysort-1650066204.4330547.json"],
    ["Full shuffle", "Partial shuffle"],
)
plot(df, end_time, "reduce_completion_times")
