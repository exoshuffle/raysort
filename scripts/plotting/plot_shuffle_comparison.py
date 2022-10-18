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
BIG_SIZE = 14

SYS = "LS"
SYS_FULL = "LibShuffle"

plt.rc("font", size=BIG_SIZE)  # controls default text sizes
plt.rc("axes", titlesize=BIG_SIZE)  # fontsize of the axes title
plt.rc("axes", labelsize=BIG_SIZE)  # fontsize of the x and y labels
plt.rc("xtick", labelsize=BIG_SIZE)  # fontsize of the tick labels
plt.rc("ytick", labelsize=BIG_SIZE)  # fontsize of the tick labels
plt.rc("legend", fontsize=MEDIUM_SIZE)  # legend fontsize
plt.rc("figure", titlesize=BIG_SIZE)  # fontsize of the figure title

# sns.set_theme(style="ticks")
sns.set_palette("Set2")
set2 = sns.color_palette("Set2")
# sns.set(font_scale=0.85)
sns.set_style("whitegrid")
sns.set_style("ticks")


def lighten(c, amount=0.75):
    import colorsys

    import matplotlib.colors as mc

    c = colorsys.rgb_to_hls(*mc.to_rgb(c))
    return colorsys.hls_to_rgb(c[0], 1 - amount * (1 - c[1]), c[2])


def plot_dask_comparison():
    columns = ["data size", "setup", "time"]
    df = pd.DataFrame(
        [
            ["1 GB", "Dask: 32 procs x 1 thread", 9.257539613],
            ["1 GB", "Dask: 8 procs x 4 threads", 9.152182102],
            ["1 GB", "Dask: 1 proc x 32 threads", 29.10137018],
            ["1 GB", "Dask-on-Ray (32 procs)", 8.962659121],
            ["10 GB", "Dask: 32 procs x 1 thread", 117.7881519],
            ["10 GB", "Dask: 8 procs x 4 threads", 112.825515],
            ["10 GB", "Dask: 1 proc x 32 threads", 356.3388017],
            ["10 GB", "Dask-on-Ray (32 procs)", 98.41430688],
            ["20 GB", "Dask: 32 procs x 1 thread", 0],
            ["20 GB", "Dask: 8 procs x 4 threads", 252.654465],
            ["20 GB", "Dask: 1 proc x 32 threads", 1327.135815],
            ["20 GB", "Dask-on-Ray (32 procs)", 186.0701251],
            ["100 GB", "Dask: 32 procs x 1 thread", 0],
            ["100 GB", "Dask: 8 procs x 4 threads", 0],
            ["100 GB", "Dask: 1 proc x 32 threads", 14221.8383],
            ["100 GB", "Dask-on-Ray (32 procs)", 1588.793045],
        ],
        columns=columns,
    )
    figname = "dask_on_ray_comp"
    return plot(
        df,
        [],
        figname,
        columns[0],
        columns[2],
        columns[1],
        "",
        "Data Size",
        "Job Time (s)",
    )


def plot_mb_all():
    columns = ["partition_size", "object_fusion", "time"]
    df = pd.DataFrame(
        [
            ["100KB", "Write (default)", 185.589769],
            ["100KB", "Write (no fusing)", 2352.518984],
            ["100KB", "Read (default)", 812.961744],
            ["100KB", "Read (no prefetching)", 1324.531656],
            ["500KB", "Write (default)", 183.376729],
            ["500KB", "Write (no fusing)", 454.441505],
            ["500KB", "Read (default)", 206.52285],
            ["500KB", "Read (no prefetching)", 371.689395],
            ["1MB", "Write (default)", 182.478411],
            ["1MB", "Write (no fusing)", 227.970836],
            ["1MB", "Read (default)", 179.591311],
            ["1MB", "Read (no prefetching)", 338.0764],
        ],
        columns=columns,
    )
    figname = "mb_all"
    set2 = sns.color_palette("Set2")
    return plot(
        df,
        [],
        figname,
        columns[0],
        columns[2],
        columns[1],
        "",
        "Object Size",
        "I/O Time (s)",
        palette=sns.color_palette(
            [set2[0], lighten(set2[0]), set2[1], lighten(set2[1])]
        ),
    )


# https://docs.google.com/spreadsheets/d/194sEiPCan_VXzOK5roMgB-7ewF4uNTnsF4eTIFmyslk/edit#gid=945817794
def plot_hdd():
    df = pd.DataFrame(
        [
            ["Spark", "2000", 1609],
            ["Spark", "1000", 1701],
            ["Spark", "500", 1558],
            [f"{SYS}-simple", "2000", 2799],
            [f"{SYS}-simple", "1000", 1929],
            [f"{SYS}-simple", "500", 1297],
            [f"{SYS}-merge", "2000", 2163],
            [f"{SYS}-merge", "1000", 1334],
            [f"{SYS}-merge", "500", 1409],
            [f"{SYS}-push", "2000", 748],
            [f"{SYS}-push", "1000", 700],
            [f"{SYS}-push", "500", 761],
            [f"{SYS}-push*", "2000", 743],
            [f"{SYS}-push*", "1000", 634],
            [f"{SYS}-push*", "500", 702],
            [f"_{SYS}-push [F]", "500", 775],
            [f"_{SYS}-push* [F]", "500", 757],
        ],
        columns=["version", "partitions", "time"],
    )
    theoretical = [339]
    return plot(
        df,
        theoretical,
        "shuffle_comparison_hdd",
        "partitions",
        "time",
        "version",
        None,  # remove legend
        "Number of Partitions",
        "Job Time (s)",
        palette=["gray", set2[0], set2[1], set2[2], set2[3], set2[2], set2[3]],
        hatches=[""] * 5 + ["////////"] * 2,
    )


# https://docs.google.com/spreadsheets/d/194sEiPCan_VXzOK5roMgB-7ewF4uNTnsF4eTIFmyslk/edit#gid=173105676
def plot_ssd():
    df = pd.DataFrame(
        [
            ["Spark", "2000", 1498],
            ["Spark", "1000", 1533],
            ["Spark", "500", 1614],
            [f"{SYS}-simple", "2000", 1085],
            [f"{SYS}-simple", "1000", 628],
            [f"{SYS}-simple", "500", 570],
            [f"{SYS}-merge", "2000", 728],
            [f"{SYS}-merge", "1000", 660],
            [f"{SYS}-merge", "500", 711],
            [f"{SYS}-push", "2000", 626],
            [f"{SYS}-push", "1000", 580],
            [f"{SYS}-push", "500", 602],
            [f"{SYS}-push*", "2000", 553],
            [f"{SYS}-push*", "1000", 533],
            [f"{SYS}-push*", "500", 596],
            [f"_{SYS}-push [F]", "500", 666],
            [f"_{SYS}-push* [F]", "500", 657],
        ],
        columns=["version", "partitions", "time"],
    )
    theoretical = [533 - 30]  # because the line is too thick
    return plot(
        df,
        theoretical,
        "shuffle_comparison_ssd",
        "partitions",
        "time",
        "version",
        "",
        "Number of Partitions",
        "Job Time (s)",
        palette=["gray", set2[0], set2[1], set2[2], set2[3], set2[2], set2[3]],
        hatches=[""] * 5 + ["////////"] * 2,
    )


def plot_large():
    df = pd.DataFrame(
        [
            [f"{SYS}-push*", "100TB", 10707 / SECS_PER_HR],
            ["Spark-push", "100TB", 19293 / SECS_PER_HR],
            ["Spark", "100TB", 30240 / SECS_PER_HR],
        ],
        columns=["version", "data_size", "time"],
    )
    theoretical = [3390 / SECS_PER_HR]
    return plot(
        df,
        theoretical,
        "shuffle_comparison_large",
        "version",
        "time",
        None,
        "",
        "",
        "Job Time (h)",
        palette=[set2[3], "darkgrey", "dimgrey"],
    )


# https://docs.google.com/spreadsheets/d/194sEiPCan_VXzOK5roMgB-7ewF4uNTnsF4eTIFmyslk/edit#gid=1160118221
def plot_small():
    df = pd.DataFrame(
        [
            ["80", "simple", 4.529596],
            ["80", "push*", 7.783609],
            ["200", "simple", 12.101356],
            ["200", "push*", 7.709302],
            ["80 ", "simple", 43.732640],
            ["80 ", "push*", 54.506559],
            ["200 ", "simple", 53.726337],
            ["200 ", "push*", 42.215111],
        ],
        columns=["partitions", "shuffle", "time"],
    )
    theoretical = []
    return plot(
        df,
        theoretical,
        "shuffle_comparison_small",
        "partitions",
        "time",
        "shuffle",
        "",
        r"10GB \hspace*{3em} 100GB",
        "Job Time (s)",
        palette=[set2[0], set2[3]] * 4,
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
    palette="Set2",
    hatches=[],
    logscale=False,
):
    g = sns.catplot(
        data=df,
        kind="bar",
        x=x,
        y=y,
        hue=hue,
        palette=palette,
        height=figheight,
        aspect=1 / golden_ratio,
    )
    fig = g.figure
    for patches, hatch in zip(g.ax.containers, hatches):
        for patch in patches:
            patch.set_hatch(hatch)
    # Add a horizontal line.
    for t in theoretical:
        plt.axhline(
            t,
            figure=fig,
            color="gray",
            linestyle="--",
            label="theoretical",
        )
    #    plt.xticks(rotation=45)
    g.despine(left=True)
    if logscale:
        g.set(yscale="log")
    g.set_axis_labels(xtitle, ytitle)
    if g.legend:
        g.legend.set_title(legend_title)
    if legend_title is None:
        g.legend.remove()
    filename = figname + ".pdf"
    print(filename)
    g.savefig(filename, bbox="tight")


plot_hdd()
plot_ssd()
plot_large()
plot_small()
# plot_mb_all()
