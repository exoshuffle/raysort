import json
import math

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

# https://scipy-cookbook.readthedocs.io/items/Matplotlib_LaTeX_Examples.html
# fig_width_pt = 241.14749  # Get this from LaTeX using \showthe\columnwidth
fig_width_pt = 350
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


# Plot the map and reduce start times
def plot():
    df = pd.read_csv("ludwig_distributed.csv")
    df["accuracy"] = df["accuracy"] * 100
    figname = "ludwig_distributed"
    fig, ax = plt.subplots(figsize=figsize)
    ax.set_ylabel("Validation accuracy", fontsize=11)
    g = sns.lineplot(
        data=df, x="time", y="accuracy", hue="run", ax=ax, markers=True, dashes=False
    )
    plt.ylim((40, 80))
    ax.yaxis.set_major_formatter(mpl.ticker.PercentFormatter(decimals=0))
    plt.grid(axis="y")
    plt.xlabel("Time (s)")
    plt.legend()
    plt.tight_layout()
    filename = figname + ".pdf"
    print(filename)
    plt.savefig(filename, bbox_inches="tight")


plot()
