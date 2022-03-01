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
sns.set(font_scale=1)


def get_microbenchmark():
    columns = ["settings", "operation", "time"]
    # Data may not be correct.
    df = pd.DataFrame(
        [
            ["Ray-default", "10 MB", 118.567094],
            ["Ray-default", "100 KB", 141.655059],
            ["Ray-default", "10 MB", 52.182254],
            ["Ray-default", "1 MB", 100],
            ["Ray-default", "100 KB", 100],
            ["Ray-default", "10 KB", 100],
            ["Ray-fusing-off", "10 MB", 118.547842],
            ["Ray-fusing-off", "100 KB", 214.00119],
            ["Ray-fusing-off", "10 MB", 52.228644],
            ["Ray-fusing-off", "1 MB", 53.304679],
            ["Ray-fusing-off", "100 KB", 59.523472],
            ["Ray-fusing-off", "10 KB", 586],
            ["Ray-prefetching-off", "Read", 0],
            ["Ray-prefetching-off", "Write", 0],
            ["Ray-prefetching-off", "10 MB", 130],
            ["Ray-prefetching-off", "1 MB", 130],
            ["Ray-prefetching-off", "0.1 MB", 130],
        ],
        columns=columns,
    )
    figname = "microbenchmark"
    return (
        df,
        [],
        figname,
        columns[0],
        columns[2],
        columns[1],
        "Block Size",
        "",
        "Run Time (s)",
        None,
        None,
    )


def get_dask_comparison():
    columns = ["data size", "setup", "time"]
    df = pd.DataFrame(
        [
            ["1 GB", "Dask: 32 procs x 1 thread", 9.257539613],
            ["1 GB", "Dask: 8 procs x 4 threads", 9.152182102],
            ["1 GB", "Dask: 1 proc x 32 threads", 29.10137018],
            ["1 GB", "Dask-on-Ray", 8.962659121],
            ["10 GB", "Dask: 32 procs x 1 thread", 117.7881519],
            ["10 GB", "Dask: 8 procs x 4 threads", 112.825515],
            ["10 GB", "Dask: 1 proc x 32 threads", 356.3388017],
            ["10 GB", "Dask-on-Ray", 98.41430688],
            ["20 GB", "Dask: 32 procs x 1 thread", 0],
            ["20 GB", "Dask: 8 procs x 4 threads", 252.654465],
            ["20 GB", "Dask: 1 proc x 32 threads", 1327.135815],
            ["20 GB", "Dask-on-Ray", 186.0701251],
            ["100 GB", "Dask: 32 procs x 1 thread", 0],
            ["100 GB", "Dask: 8 procs x 4 threads", 0],
            ["100 GB", "Dask: 1 proc x 32 threads", 14221.8383],
            ["100 GB", "Dask-on-Ray", 1588.793045],
        ],
        columns=columns,
    )
    figname = "dask_on_ray_comp"
    return (
        df,
        [],
        figname,
        columns[0],
        columns[2],
        columns[1],
        "",
        "",
        "Job Completion Time (s)",
        None,
        None,
    )


def get_obj_fusion_time():
    columns = ["partition_size", "object_fusion", "time"]
    df = pd.DataFrame(
        [
            #            ["1GB", "With fusion (default)", 0],
            #            ["1GB", "Without fusion", 0],
            ["2GB", "With fusion (default)", 758.968],
            ["2GB", "Without fusion", 808.523],
        ],
        columns=columns,
    )
    figname = "obj_fusion_runtime"
    return (
        df,
        [],
        figname,
        columns[0],
        columns[2],
        columns[1],
        "",
        "",
        "Job Completion Time (s)",
        None,
        28,
    )


def get_obj_fusion_throughput():
    columns = ["partition_size", "object_fusion", "spill_throughput"]
    df = pd.DataFrame(
        [
            #            ["1GB", "With fusion (default)", 0],
            #            ["1GB", "Without fusion", 0],
            ["2GB", "With fusion (default)", 1767],
            ["2GB", "Without fusion", 1084],
        ],
        columns=columns,
    )
    figname = "obj_fusion_throughput"
    return (
        df,
        [],
        figname,
        columns[0],
        columns[2],
        columns[1],
        "",
        "",
        "Spill Throughput (MiB/s)",
        None,
        28,
    )


def get_pipelined_fetch():
    columns = ["partition_size", "arg_pipelined", "runtime"]
    df = pd.DataFrame(
        [
            ["2GB", "Pipelined (default)", 758.968],
            ["2GB", "Not pipelined", 808.444],
        ],
        columns=columns,
    )
    figname = "arg_fetching"
    return (
        df,
        [],
        figname,
        columns[0],
        columns[2],
        columns[1],
        "Argument Fetching",
        "",
        "Job Completion Time (s)",
        None,
    )


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
        "",
        "",
        "Job Completion Time (s)",
        None,
    )


# https://docs.google.com/spreadsheets/d/1ia36j5ECKLde5J22DvNMrwBSZj85Fz7gNRiJJvK_qLA/edit#gid=0
def get_data_ssd():
    df = pd.DataFrame(
        [
            ["Ray-simple", "2GB", 758.968],
            ["Ray-simple", "0.5GB", 1308.5],
            ["Ray-simple", "0.1GB", 0],
            ["Ray-Riffle", "2GB", 1002.35],
            ["Ray-Riffle", "0.5GB", 1031.096],
            ["Ray-Riffle", "0.1GB", 1141.065],
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
    return (
        df,
        theoretical,
        "shuffle_comparison",
        "version",
        "time",
        "partition_size",
        "Partition Size",
        "",
        "Job Completion Time (s)",
        None,
    )


def get_data_nvme():
    df = pd.DataFrame(
        [
            ["Ray-simple", "2GB", 392.003],
            ["Ray-simple", "0.5GB", 609.277],
            ["Ray-simple", "0.1GB", 0],
            ["Ray-Riffle", "2GB", 482.203],
            ["Ray-Riffle", "0.5GB", 489.848],
            ["Ray-Riffle", "0.1GB", 0],
            ["Ray-Magnet", "2GB", 415.271],
            ["Ray-Magnet", "0.5GB", 461.591],
            ["Ray-Magnet", "0.1GB", 557.691],
            ["Ray-Cosco", "2GB", 378.239],
            ["Ray-Cosco", "0.5GB", 399.196],
            ["Ray-Cosco", "0.1GB", 511.533],
            ["Spark", "2GB", 833.298],
            ["Spark", "0.5GB", 846.917],
            ["Spark", "0.1GB", 861.462],
        ],
        columns=["version", "partition_size", "time"],
    )
    theoretical = [339.084]
    return (
        df,
        theoretical,
        "shuffle_comparison_nvme",
        "version",
        "time",
        "partition_size",
        "Partition Size",
        "",
        "Job Completion Time (s)",
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
    fontsize=None,
):
    if fontsize:
        TINY_SIZE = 16
        SMALL_SIZE = fontsize - 3
        MEDIUM_SIZE = fontsize
        plt.rc("font", size=SMALL_SIZE)  # controls default text sizes
        plt.rc("axes", titlesize=MEDIUM_SIZE)  # fontsize of the axes title
        plt.rc("axes", labelsize=TINY_SIZE)  # fontsize of the x and y labels
        plt.rc("xtick", labelsize=SMALL_SIZE)  # fontsize of the tick labels
        plt.rc("ytick", labelsize=SMALL_SIZE)  # fontsize of the tick labels
        plt.rc("legend", fontsize=SMALL_SIZE)
        plt.rcParams.update({"font.size": fontsize})

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


for data in [get_data_ssd(), get_data_nvme(), get_data_ft(), get_obj_fusion_time()]:
    plot(*data)
