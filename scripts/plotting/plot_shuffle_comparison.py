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
BIG_SIZE = 14

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


def plot_dask_comparison():
    columns = ["data size", "setup", "time"]
    df = pd.DataFrame(
        [
            ["1", "Dask (32 x 1)", 9.257539613],
            ["1", "Dask (8 x 4)", 9.152182102],
            ["1", "Dask (1 x 32)", 29.10137018],
            ["1", "Dask-on-Ray", 8.962659121],
            ["10", "Dask (32 x 1)", 117.7881519],
            ["10", "Dask (8 x 4)", 112.825515],
            ["10", "Dask (1 x 32)", 356.3388017],
            ["10", "Dask-on-Ray", 98.41430688],
            ["20", "Dask (32 x 1)", 0],
            ["20", "Dask (8 x 4)", 252.654465],
            ["20", "Dask (1 x 32)", 1327.135815],
            ["20", "Dask-on-Ray", 186.0701251],
            ["100", "Dask (32 x 1)", 0],
            ["100", "Dask (8 x 4)", 0],
            ["100", "Dask (1 x 32)", 14221.8383],
            ["100", "Dask-on-Ray", 1588.793045],
        ],
        columns=columns,
    )
    figname = "dask_on_ray_comp"
    text_kwargs = dict(
        fontsize=14,
        fontweight="black",
        ha="center",
        va="center",
    )
    return plot(
        df,
        [],
        figname,
        columns[0],
        columns[2],
        columns[1],
        "",
        "Data Size (GB)",
        "Job Time (s)",
        palette=[
            "silver",
            "gray",
            "dimgray",
            set2[0],
        ],
        texts=[
            ((1.70, 9, "X"), dict(**text_kwargs, color="silver")),
            ((2.72, 9, "X"), dict(**text_kwargs, color="silver")),
            ((2.90, 9, "X"), dict(**text_kwargs, color="gray")),
        ],
        logscale=True,
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
            [
                lighten(set2[0], 1.1),
                lighten(set2[0]),
                lighten(set2[1], 1.9),
                lighten(set2[1], 1.4),
            ]
        ),
        legend_kwargs=dict(bbox_to_anchor=(0.5, 0.6)),
    )


# https://docs.google.com/spreadsheets/d/194sEiPCan_VXzOK5roMgB-7ewF4uNTnsF4eTIFmyslk/edit#gid=945817794
def plot_hdd():
    df = pd.DataFrame(
        [
            ["Magnet", "2000", 1458],
            ["Magnet", "1000", 1599],
            ["Magnet", "500", 1572],
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
        palette=[
            "dimgray",
            lighten(set2[0], 0.5),
            set2[1],
            lighten(set2[2], 1.3),
            lighten(set2[3], 2.8),
            lighten(set2[2], 1.3),
            lighten(set2[3], 2.8),
        ],
        hatches=[""] * 5 + ["////////"] * 2,
    )


# https://docs.google.com/spreadsheets/d/194sEiPCan_VXzOK5roMgB-7ewF4uNTnsF4eTIFmyslk/edit#gid=173105676
def plot_ssd():
    df = pd.DataFrame(
        [
            ["Magnet", "2000", 1652],
            ["Magnet", "1000", 1680],
            ["Magnet", "500", 1607],
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
        palette=[
            "dimgray",
            lighten(set2[0], 0.5),
            set2[1],
            lighten(set2[2], 1.3),
            lighten(set2[3], 2.8),
            lighten(set2[2], 1.3),
            lighten(set2[3], 2.8),
        ],
        hatches=[""] * 5 + ["////////"] * 2,
    )


def plot_large():
    df = pd.DataFrame(
        [
            [f"{SYS}-push*", "100TB", 10707 / SECS_PER_HR],
            ["Magnet", "100TB", 19293 / SECS_PER_HR],
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
        palette=[lighten(set2[2], 1.3), "darkgrey", "dimgrey"],
    )


# https://docs.google.com/spreadsheets/d/194sEiPCan_VXzOK5roMgB-7ewF4uNTnsF4eTIFmyslk/edit#gid=1160118221
def plot_small():
    df = pd.DataFrame(
        [
            ["100GB", f"{SYS}-simple", 39],
            ["100GB", f"{SYS}-merge", 66],
            ["100GB", f"{SYS}-push", 53],
            ["100GB", f"{SYS}-push*", 50],
            ["200GB", f"{SYS}-simple", 96],
            ["200GB", f"{SYS}-merge", 160],
            ["200GB", f"{SYS}-push", 143],
            ["200GB", f"{SYS}-push*", 130],
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
        "Data Size (100 partitions)",
        "Job Time (s)",
        palette=[
            lighten(set2[0], 0.5),
            set2[1],
            lighten(set2[2], 1.3),
            lighten(set2[3], 2.8),
        ]
        * 2,
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
    texts=[],
    logscale=False,
    legend_kwargs={},
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
    for args, kwargs in texts:
        g.ax.text(*args, **kwargs)
    g.despine(left=True)
    plt.xlabel(xtitle, fontsize=20)
    plt.ylabel(ytitle, fontsize=20)
    plt.xticks(fontsize=18)
    plt.yticks(fontsize=18)
    if logscale:
        g.set(yscale="log")
    # Legend.
    if g.legend:
        if legend_title is None:
            g.legend.remove()
        else:
            kwargs = dict(
                title=legend_title,
                bbox_to_anchor=(0.75, 0.5),
                fontsize=16,
            )
            kwargs.update(legend_kwargs)
            sns.move_legend(
                g,
                "center left",
                **kwargs,
            )
    # Add a horizontal line.
    for t in theoretical:
        plt.axhline(
            t,
            figure=fig,
            color="gray",
            linestyle="--",
            label="theoretical",
        )
    filename = figname + ".pdf"
    print(filename)
    g.savefig(filename, bbox_inches="tight")


plot_dask_comparison()
plot_hdd()
plot_ssd()
plot_large()
plot_small()
plot_mb_all()
