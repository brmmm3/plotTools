# plotTools
Some useful tools for Burst coin

plotChecker: BURST plot checker (version 1.0)
    Checking and validation of plots for BURST
    Translated into Python 3.6 from Blago's C++ version which is only for Windows.

plotOptimizer: BURST plot optimizer (version 1.0)
    Optimizing plot files. It is the same as in repo mdcct, but in Python 3.6.

plotMerger: BURST plot merger (version 1.0)
    Merge several plot files into a single one.
    Unoptimized plots are also accepted.
    Missing nonces are automatically computed through calling the plotter (plot64 from cg_obup).
    Usage: plotMerger [-p PlotterPath] [-x PlotCore] [-d] [-o OUTDIR] INPATH1 INPATH2 ...
    INPATHx can be the path to a plot file or a directory to a set of plot files.
