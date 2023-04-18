import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

# https://scipy-cookbook.readthedocs.io/items/Matplotlib_LaTeX_Examples.html
# Get fig_width_pt from LaTeX using \the\columnwidth
fig_width_pt = 350  # 240.94499  # acmart-SIGPLAN
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

SMALL_SIZE = 9
MEDIUM_SIZE = 12
BIG_SIZE = 16

SYS = "LS"
SYS_FULL = "LibShuffle"

plt.rc("font", size=BIG_SIZE)  # controls default text sizes
plt.rc("axes", titlesize=BIG_SIZE)  # fontsize of the axes title
plt.rc("axes", labelsize=BIG_SIZE)  # fontsize of the x and y labels
plt.rc("xtick", labelsize=BIG_SIZE)  # fontsize of the tick labels
plt.rc("ytick", labelsize=BIG_SIZE)  # fontsize of the tick labels
plt.rc("legend", fontsize=BIG_SIZE)  # legend fontsize
plt.rc("figure", titlesize=BIG_SIZE)  # fontsize of the figure title

sns.set_style("ticks")
sns.set_palette("Set2")
set2 = sns.color_palette("Set2")


def lighten(c, amount=0.75):
    import colorsys

    import matplotlib.colors as mc

    c = colorsys.rgb_to_hls(*mc.to_rgb(c))
    return colorsys.hls_to_rgb(c[0], 1 - amount * (1 - c[1]), c[2])


def plot_cloudsort():
    df = pd.DataFrame(
        [
            [2016, 1.44, "NADSort"],
            [2022, 1.15, "NADSort"],
            [2022, 0.97, "LibShuffle-CloudSort"],
            # [2022, 0.74, "Theoretical Limit"],
        ],
        columns=["year", "time", "system"],
    )
    ax = sns.scatterplot(
        data=df,
        x="year",
        y="time",
        hue="system",
        palette=["gray", set2[2]],
        style="system",
        markers={"NADSort": "s", "LibShuffle-CloudSort": "D"},
        s=100,
    )
    ax.set(ylim=(0, 1.50))
    # prepend y axis labels with a dollar sign
    ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter("\\${x:,.2f}"))
    # draw a line between the first two points
    ax.plot([2016, 2022], [1.44, 1.15], color="gray", linewidth=2, linestyle="--")
    # remove the legend title
    ax.legend().set_title(None)
    # despine
    sns.despine()

    fig = ax.get_figure()
    plt.xlabel("Year", fontsize=20)
    plt.ylabel("Cost/TB", fontsize=20)
    plt.xticks(fontsize=18)
    plt.yticks(fontsize=18)
    plt.legend(fontsize=20)
    filename = "cloudsort.pdf"
    print(filename)
    fig.savefig(filename, bbox_inches="tight")


plot_cloudsort()
