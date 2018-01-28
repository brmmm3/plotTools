# plotTools
Some useful tools for Burst coin plot files.
The tools are written in Python 3.6 and should work on any platform.
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

## plotWizard: Burst plot wizard (version 1.0)
   Helps you to automatically fill disks with plot files.
   ###### Usage:
    plotWizard.py -k Key [-p PlotterPath] [-x PlotCore] [-m MaxMemUsage] [-M CreepMinerPath] [-c Path2MiningConf] [-R] [-s MinPlotSize] [-S MaxPlotSize] [-f MinDiskFree] [-d] [-C PlotWizardConf] [-t TMPDIR] [PLOTSDIR1] [PLOTSDIR2]...
   ###### Options:
    -C = Path to plotWizard's configuration file.
    -p = Path to cg_obup plotter executable.
    -m = Maximum RAM usage (Default = 4 GB).
    -M = Path to creepMiner executable.
    -c = Path to mining.conf for creepMiner.
    -R = Restart creepMiner after each successfully created plot file.
    -s = Minimum size of plot files (Default = 10 GB).
    -S = Maximum size of plot files (Default = 1 TB).
    -f = Minimum free size disk space to keep (Default is 0).
        Example: 4G,/media/plot1:10G -> For /media/plot1 10G, for all others 4G
    -d Dry run.
    -t = Temporary directory for plot file creation.
    PLOTSDIRx directories for plot files.


#### Python 3.6
If you are using Debian Stretch you will need python3.6.
This is packaged for testing already, so you can use testing.
To do so, execute those things as root:
```
# Add Testing Repository
echo "deb http://ftp.debian.org/debian testing main non-free contrib" > /etc/apt/sources.list.d/testing
# Get your current installed codename
codename=$(lsb_release -c |awk '{print $2}')
# Pin your current installation to $codename
echo "APT::Default-Release \"${codename}\";" > /etc/apt/apt.conf.d/99DefaultRLS
# Update your repository infos
apt-get update
# Install python3.6 from testing
apt-get install python3.6 -t testing
```
You may want to say yes to all questions asked while installing python3.6

If you are using Ubuntu or and distribution which is based on Ubuntu you may add the following ppa:
```
sudo add-apt-repository ppa:jonathonf/python-3.6
sudo apt-get update
sudo apt install python3.6
```

For colored output you may also want to install colorama, but this is optional.
```
pip3.6 install --user colorama
```
