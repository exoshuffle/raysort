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


def get_data_large():
    df = pd.DataFrame(
        [
            ["Spark", "10TB", 3140.228 / SECS_PER_HR],
            [r"\textsf{Exoshuffle}", "10TB", 1469.88 / SECS_PER_HR],
            ["(Theoretical)", "10TB", 1271.566 / SECS_PER_HR],
            ["Spark", "50TB", 10741.151 / SECS_PER_HR],
            [r"\textsf{Exoshuffle}", "50TB", 8042.029 / SECS_PER_HR],
            ["(Theoretical)", "50TB", 6357.829 / SECS_PER_HR],
            ["Spark", "100TB", 0 / SECS_PER_HR],
            [r"\textsf{Exoshuffle}", "100TB", 15593.347 / SECS_PER_HR],
            ["(Theoretical)", "100TB", 12715.658 / SECS_PER_HR],
        ],
        columns=["version", "data_size", "time"],
    )
    figname = "shuffle_comparison_large"
    return (
        df,
        [],
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
    g.savefig(filename, bbox_inches="tight")


for data in [get_data_large()]:
    plot(*data)
