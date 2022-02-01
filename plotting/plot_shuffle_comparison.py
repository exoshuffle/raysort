import itertools

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

plt.rcParams.update(
    {
        "figure.figsize": figsize,
        "figure.dpi": 150,
        "text.usetex": True,
    }
)

SECS_PER_MIN = 60
SECS_PER_HR = 3600

SMALL_SIZE = 8
MEDIUM_SIZE = 10
BIG_SIZE = 12

plt.rc("font", size=SMALL_SIZE)  # controls default text sizes
plt.rc("axes", titlesize=SMALL_SIZE)  # fontsize of the axes title
plt.rc("axes", labelsize=SMALL_SIZE)  # fontsize of the x and y labels
plt.rc("xtick", labelsize=SMALL_SIZE)  # fontsize of the tick labels
plt.rc("ytick", labelsize=SMALL_SIZE)  # fontsize of the tick labels
plt.rc("legend", fontsize=SMALL_SIZE)  # legend fontsize
plt.rc("figure", titlesize=BIG_SIZE)  # fontsize of the figure title

sns.set_theme(style="ticks")
sns.set_palette("Set2")


def get_data_ft():
    df = pd.DataFrame(
        [
            ["Ray-Riffle", "No failure", 1002.35],
            ["Ray-Riffle", "With failure", 1556.876],
            ["Ray-Magnet", "No failure", 988.296],
            ["Ray-Magnet", "With failure", 1026.939],
            ["Ray-Cosco", "No failure", 827.059],
            ["Ray-Cosco", "With failure", 881.596],
        ],
        columns=["version", "fail_mode", "time"],
    )
    figname = "ft_comparison"
    return (
        df,
        [],
        figname,
        "version",
        "time",
        "fail_mode",
        "Failure Mode",
        "",
        "Job Completion Time (s)",
        None,
    )

# https://docs.google.com/spreadsheets/d/1ia36j5ECKLde5J22DvNMrwBSZj85Fz7gNRiJJvK_qLA/edit#gid=0
def get_data_small():
    df = pd.DataFrame(
        [
            ["Ray-simple", "2GB", 758.968],
            ["Ray-simple", "0.5GB", 1096.112],
            ["Ray-simple", "0.1GB", 0],
            ["Ray-Riffle", "2GB", 1002.35],
            ["Ray-Riffle", "0.5GB", 1031.096],
            ["Ray-Riffle", "0.1GB", 2817.34],
            ["Ray-Magnet", "2GB", 988.296],
            ["Ray-Magnet", "0.5GB", 984.41],
            ["Ray-Magnet", "0.1GB", 1112.63],
            ["Ray-Cosco", "2GB", 827.059],
            ["Ray-Cosco", "0.5GB", 779.383],
            ["Ray-Cosco", "0.1GB", 988.836],
            ["Spark", "2GB", 925.232],
            ["Spark", "0.5GB", 995.099],
            ["Spark", "0.1GB", 850.577],
        ],
        columns=["version", "partition_size", "time"],
    )
    theoretical = [813.802]
    figname = "shuffle_comparison"
    return (
        df,
        theoretical,
        figname,
        "version",
        "time",
        "partition_size",
        "Partition Size",
        "",
        "Job Completion Time (s)",
        None,
    )


def get_data_large():
    df = pd.DataFrame(
        [
            ["Spark", "10TB", 3140.228 / SECS_PER_HR],
            ["Ray-opt", "10TB", 1469.88 / SECS_PER_HR],
            ["Spark", "50TB", 0 / SECS_PER_HR],
            ["Ray-opt", "50TB", 8042.029 / SECS_PER_HR],
            ["Spark", "100TB", 0 / SECS_PER_HR],
            ["Ray-opt", "100TB", 15593.347 / SECS_PER_HR],
        ],
        columns=["version", "data_size", "time"],
    )
    theoretical = [
        1271.566 / SECS_PER_HR,
        6357.829 / SECS_PER_HR,
        12715.658 / SECS_PER_HR,
    ]
    figname = "shuffle_comparison_large"
    return (
        df,
        theoretical,
        figname,
        "data_size",
        "time",
        "version",
        "",
        "Total Data Size",
        "Job Completion Time (h)",
        None,
    )


def plot(
    df,
    theoretical,
    figname,
    x,
    y,
    hue,
    legend_title,
    xtitle,
    ytitle,
    palette,
):
    g = sns.catplot(
        data=df,
        kind="bar",
        x=x,
        y=y,
        hue=hue,
        palette=palette,
        height=3,
        aspect=1 / golden_ratio,
    )
    fig = g.figure
    ax = fig.gca()
    # # Add hatches to bars.
    # hatches = itertools.cycle(["", "/", "\\"])
    # for i, bar in enumerate(ax.patches):
    #     if i % 3 == 0:
    #         hatch = next(hatches)
    #     bar.set_hatch(hatch)
    # Add a horizontal line.
    for t in theoretical:
        plt.axhline(
            t,
            figure=fig,
            color="gray",
            linestyle="--",
            label="theoretical",
        )
    g.despine(left=True)
    g.set_axis_labels(xtitle, ytitle)
    g.legend.set_title(legend_title)
    filename = figname + ".pdf"
    print(filename)
    g.savefig(filename)


for data in [get_data_small(), get_data_ft()]:
    plot(*data)
