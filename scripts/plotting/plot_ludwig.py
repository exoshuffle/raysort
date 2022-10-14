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
colors = sns.color_palette("Set2")


# Plot the map and reduce start times
def plot(figname, palette=None):
    df = pd.read_csv(f"{figname}.csv")
    df["accuracy"] = df["accuracy"] * 100
    df["time"] = df["time"] / 60
    fig, ax = plt.subplots(figsize=figsize)
    ax.set_ylabel("Accuracy", fontsize=11)
    g = sns.lineplot(
        data=df,
        x="time",
        y="accuracy",
        hue="run",
        ax=ax,
        style="run",
        markers=["o", "v"],
        palette=palette,
    )
    plt.ylim((40, 80))
    ax.yaxis.set_major_formatter(mpl.ticker.PercentFormatter(decimals=0))
    plt.grid(axis="y")
    plt.xlabel("Time (min)")
    plt.legend()
    plt.tight_layout()
    filename = figname + ".pdf"
    print(filename)
    plt.savefig(filename, bbox_inches="tight")


plot("ludwig_single")
plot("ludwig_distributed", palette=[colors[0], colors[2]])
