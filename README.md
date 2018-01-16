# plotTools
Some useful tools for Burst coin plot files.
The tools are written in Python 3 and should work on any platform.
###### For plotMerger there is currently one limitation:
    In case of holes (missing nonces) between plot files for merging only the plotter from repo cg_obup is supported.

## plotChecker: Burst plot checker (version 1.0)
   Checking and validation of plots for Burst
   Translated into Python 3.6 from Blago's C++ version which is only for Windows.

## plotOptimizer: Burst plot optimizer (version 1.0)
   Optimizing plot files. It is the same as in repo mdcct, but in Python 3.6.

## plotMerger: Burst plot merger (version 1.0)
   Merge several plot files into a single one.
   Unoptimized plots are also accepted.
   Missing nonces are automatically computed through calling the plotter (plot64 from cg_obup).
   ###### Usage:
    plotMerger.py [-p PlotterPath] [-x PlotCore] [-r] [-d] [-o OUTDIR] [-t TMPDIR] INPATH1 INPATH2 ...
   ###### Options:
    -p Path to plotter executable (currently only plotter from repo cg_obup is supported).
    -x Core to use for plotting (1=SSE, 1=AVX (default), 0=do not use -> currently broken!)
    -r Remove old plot files after successfull merge.
    -o Optional output directory. If omitted the output is written to same directory as input with lowest start nonce.
    -d Dry run.
    INPATHx can be the path to a plot file or a directory to a set of plot files.

## plotSplitter: Burst plot splitter (version 1.0)
   Split a plot file into smaller plot files.
   An unoptimized plot as input is also accepted. The output will be optimized plot files.
   ###### Usage:
    plotSplitter.py [-r] [-t] [-d] [-s size] [-o OUTDIR] INPATH
   ###### Options:
    -r Remove old plot file after successfull split.
    -t Truncate plot file instead of splitting it.
    -s Destination size of splitted plot files (either nonce count or size with K|M|G|T).
    -o Optional output directory. If omitted the output is written to same directory as input.
    -d Dry run.
