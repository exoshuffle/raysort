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

plt.rcParams.update(
    {
        "axes.titlesize": 14,
        "axes.labelsize": 14,
        "font.size": 14,
        "figure.figsize": figsize,
        "figure.dpi": 150,
        "legend.fontsize": 16,
        "text.usetex": True,
        "xtick.labelsize": 16,
        "ytick.labelsize": 16,
        "mathtext.fontset": "custom",
        "mathtext.rm": "Bitstream Vera Sans",
        "mathtext.it": "Bitstream Vera Sans:italic",
        "mathtext.bf": "Bitstream Vera Sans:bold",
    }
)

# sns.set_theme(style="ticks", font_scale=1)
sns.set_style("ticks")
sns.set_palette("Set2")


def lighten(c, amount=0.75):
    import colorsys

    import matplotlib.colors as mc

    c = colorsys.rgb_to_hls(*mc.to_rgb(c))
    return colorsys.hls_to_rgb(c[0], 1 - amount * (1 - c[1]), c[2])


# Read in json file of timestamps for map and reduce tasks
def get_json_input(fname, run):
    with open(fname) as f:
        data = json.load(f)
    map_times = []
    reduce_times = []
    max_end = 0
    start = math.inf
    for row in data:
        t = float(row["ts"] + row["dur"]) / 1000000.0  # convert to seconds
        start_t = float(row["ts"]) / 1000000.0
        if row["name"] == "mapper":
            map_times.append(t)
        elif row["name"] == "reducer":
            reduce_times.append(t)
        if t > max_end:
            max_end = t
        if start_t < start:
            start = start_t
    map_times.sort()
    reduce_times.sort()
    num_map_tasks = len(map_times)  # num map tasks = num mappers = num reducers.
    map_data = [
        (i * 100 / num_map_tasks, t - start, "map", run)
        for i, t in enumerate(map_times, start=1)
    ]
    num_reduce_tasks = len(reduce_times)  # num map tasks = num mappers = num reducers.
    reduce_data = [
        (i * 100 / num_reduce_tasks, t - start, "reduce", run)
        for i, t in enumerate(reduce_times, start=1)
    ]
    reduce_data.insert(0, (0, reduce_data[0][1], "reduce", run))

    map_data.append((100, max_end - start, "map", run))
    reduce_data.append((100, max_end - start, "reduce", run))

    df_map = pd.DataFrame(map_data, columns=["pct", "time", "Task", "run"])
    df_reduce = pd.DataFrame(reduce_data, columns=["pct", "time", "Task", "run"])
    max_end = max_end - start
    print(max_end)
    return df_map, df_reduce, max_end


# Plot the map and reduce start times
def plot(
    df_simple_map,
    df_simple_reduce,
    df_streaming_map,
    df_streaming_reduce,
    end_time_simple,
    end_time_streaming,
    df_error_metric,
    figname,
    x="time",
    y="pct",
    hue="run",
):
    colors = sns.color_palette("Set2")
    fig, ax = plt.subplots(figsize=figsize)
    g = sns.lineplot(
        data=df_simple_map,
        x=x,
        y=y,
        hue=hue,
        palette=[colors[0]],
        ax=ax,
        linestyle=":",
        linewidth=3,
    )
    g = sns.lineplot(
        data=df_streaming_map,
        x=x,
        y=y,
        hue=hue,
        palette=[lighten(colors[1], 1.5)],
        ax=ax,
        linestyle=":",
    )
    g = sns.lineplot(
        data=df_simple_reduce,
        x=x,
        y=y,
        hue=hue,
        palette=[colors[0]],
        legend=False,
        ax=ax,
        linewidth=2,
    )
    g = sns.lineplot(
        data=df_streaming_reduce,
        x=x,
        y=y,
        hue=hue,
        palette=[lighten(colors[1], 1.5)],
        legend=False,
        ax=ax,
    )
    g = sns.lineplot(
        data=df_error_metric,
        x=x,
        y=y,
        hue=hue,
        palette=["gray"],
        ax=ax,
    )
    sns.move_legend(
        g,
        "center left",
        title=None,
        bbox_to_anchor=(1, 0.5),
        fontsize=16,
    )
    plt.xlabel("Time (s)")
    plt.ylabel("% Tasks completed")
    plt.xlim((0, int(math.ceil(max(end_time_simple, end_time_streaming)))))
    plt.ylim((0, 100))
    ax.yaxis.set_major_formatter(mpl.ticker.PercentFormatter(decimals=0))
    plt.xticks()
    plt.yticks()
    plt.grid(axis="y")
    filename = figname + ".pdf"
    print(filename)
    plt.savefig(filename, bbox_inches="tight")


def get_error_metric(df_streaming, fname):
    with open(fname) as fin:
        data = [json.loads(line) for line in fin.readlines()]

    all_keys = set().union(*[set(d.keys()) for d in data])
    key_map = {k: i for i, k in enumerate(sorted(all_keys))}

    def make_vector(d: dict[str, float], key_to_id: dict[str, int]) -> np.ndarray:
        vec = np.zeros(len(key_to_id))
        for k, v in d.items():
            vec[key_to_id[k]] = v
        return vec

    p_vectors = [make_vector(d, key_map) for d in data]
    truth_vector = p_vectors[-1]

    def kl_divergence(x: np.ndarray, y: np.ndarray) -> float:
        eps = 1e-9
        return np.sum(x * np.log(x / (y + eps) + eps))

    error_fn = kl_divergence
    errors = [error_fn(p, truth_vector) for p in p_vectors]
    return get_error_metric_df(df_streaming, errors)


def get_error_metric_df(df_streaming, errors):
    num_tasks_per_round = 10
    times = df_streaming["time"].iloc[range(0, len(df_streaming), num_tasks_per_round)]
    num_rounds = len(times)
    errors = np.array(errors[:num_rounds]) * 100
    return pd.DataFrame({"time": times, "pct": errors, "run": "Approx Error"})


df_simple_map, df_simple_reduce, end_time_simple = get_json_input(
    "streaming/pageviews-simple.json", "Simple"
)
df_streaming_map, df_streaming_reduce, end_time_streaming = get_json_input(
    "streaming/pageviews-streaming.json", "Streaming"
)
error_metric = get_error_metric(df_streaming_reduce, "streaming/pageviews.jsonl")
plot(
    df_simple_map,
    df_simple_reduce,
    df_streaming_map,
    df_streaming_reduce,
    end_time_simple,
    end_time_streaming,
    error_metric,
    "streaming_shuffle",
)
