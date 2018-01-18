#!/usr/bin/python3

import os
import sys
import time
import json
import _thread
from queue import Queue, Empty
from subprocess import PIPE, Popen
import psutil
from util import diskFree, NONCE_SIZE

MAX_READ = 4 * NONCE_SIZE
MB = 1024 * 1024
GB = 1024 * MB

ON_POSIX = 'posix' in sys.builtin_module_names


def enqueue_output(out, queue):
    for line in iter(out.readline, b''):
        line = line.strip()
        if line:
            queue.put(line.decode("utf-8"))
    out.close()


def execute(cmdLine, cwd = None, lock = None):
    print(BRIGHTBLUE + "RUN:  " + " ".join(cmdLine) + RESET_ALL)
    if bDryRun or bCancel:
        return
    prc = Popen(" ".join(cmdLine), stdout = PIPE, stderr = PIPE, bufsize = 1, close_fds = ON_POSIX, shell = True, cwd = cwd)
    stdout = Queue()
    tStdOut = Thread(target = enqueue_output, args = ( prc.stdout, stdout ), daemon = True)
    tStdOut.start()
    stderr = Queue()
    tStdErr = Thread(target = enqueue_output, args = ( prc.stderr, stderr ), daemon = True)
    tStdErr.start()
    while not bCancel and prc.poll() is None and lock is None or lock.locked():
        while True:
            try:
                line = stdout.get(timeout = 0.1)
            except Empty:
                break
            else:
                print(BRIGHTGREEN + line + RESET_ALL)
        while True:
            try:
                line = stderr.get(timeout = 0.1)
            except Empty:
                break
            else:
                print(BRIGHTRED + line + RESET_ALL)
    if not lock is None and not lock.locked():
        prc.terminate()
    if prc.returncode:
        print(BRIGHTRED + f"Error: Command returned with error code {prc.returncode}!" + RESET_ALL)
        if lock is None:
            sys.exit(1)


def createPlotFilesThread(plotFiles, moveFiles):
    cwd = os.path.dirname(plotterPathName)
    while not bCancel:
        if not plotFiles:
            time.sleep(0.1)
            continue
        plotFile = plotFiles.popleft()
        if plotFile is None:
            break
        plotFileNum, startNonce, nonces, dstDirName = plotFile
        dirName = tmpDirName if tmpDirName else dstDirName
        size = nonces * NONCE_SIZE
        while not bCancel and not bDryRun and (diskFree(dirName) < size):
            print(BRIGHTBLUE + f"Waiting for enough free disk space in {dirName}..." + RESET_ALL)
            time.sleep(1.0)
        if bCancel:
            break
        fileName = f"{key}_{startNonce}_{nonces}_{nonces}"
        print(BRIGHTYELLOW
              + f"{plotFileNum}/{plotFileCnt} Creating plot file {fileName} with size {nonces * NONCE_SIZE // GB} GB in {dirName}..."
              + RESET_ALL)
        if not bDryRun:
            cmdLine = [ plotterPathName, "-k", str(key), "-d", dirName, "-t", str(threadCnt), "-x", plotCore,
                        "-s", str(startNonce), "-n", str(nonces), "-m", str(plotMemUsage // NONCE_SIZE) ]
            execute(cmdLine, cwd)
        if dirName != dstDirName:
            moveFiles.append(( plotFileNum, fileName, dirName, dstDirName ))
    moveFiles.append(None)
    print(BRIGHTBLUE + "createPlotFilesThread finished." + RESET_ALL)


def movePlotFilesThread(moveFiles, minerLock):
    while not bCancel:
        if not moveFiles:
            time.sleep(0.1)
            continue
        moveFile = moveFiles.popleft()
        if moveFile is None:
            break
        plotFileNum, fileName, srcDirName, dstDirName = moveFile
        srcPathName = os.path.join(srcDirName, fileName)
        print(BRIGHTYELLOW
              + f"{plotFileNum}/{plotFileCnt} Moving plot file {fileName} from {srcDirName} to {dstDirName}..."
              + RESET_ALL)
        if not bDryRun:
            execute([ "dd", f"if={srcPathName}", f"of={os.path.join(dstDirName, fileName)}",  "bs=1M", "status=progress" ])
            os.remove(srcPathName)
        if bRestartMiner and minerLock.locked():
            minerLock.release()
    if minerLock.locked():
        minerLock.release()
    print(BRIGHTBLUE + "movePlotFilesThread finished." + RESET_ALL)


def minerThread(minerLock):
    while not bCancel:
        for prc in psutil.process_iter():
            try:
                pInfo = prc.as_dict(attrs = [ 'pid', 'name' ])
            except psutil.NoSuchProcess:
                pass
            else:
                if "creepMiner" in pInfo["name"]:
                    prc.kill()
                    time.sleep(1.0)
                    if psutil.pid_exists(pInfo[ "pid" ]):
                        prc.terminate()
                        time.sleep(1.0)
                    break
        minerLock.acquire()
        execute([ minerPathName ], os.path.dirname(minerPathName), minerLock)
    print(BRIGHTBLUE + "minerThread finished." + RESET_ALL)


def findMountPoint(pathName):
    pathName = os.path.abspath(pathName)
    while not os.path.ismount(pathName):
        pathName = os.path.dirname(pathName)
    return pathName


def arg2Int(arg, factor = 1):
    for c, m in (("k", 1024), ("m", MB), ("g", 1024 * MB), ("t", MB * MB)):
        if arg.endswith(c) or arg.endswith(c.upper()):
            return int(arg[:-1]) * m
    return int(arg) * factor


if __name__ == "__main__":
    import sys
    from collections import deque
    from threading import Thread
    try:
        from colorama import init, Fore, Style
        BRIGHTRED = Style.BRIGHT + Fore.RED
        BRIGHTGREEN = Style.BRIGHT + Fore.GREEN
        BRIGHTBLUE = Style.BRIGHT + Fore.BLUE
        BRIGHTYELLOW = Style.BRIGHT + Fore.YELLOW
        RESET_ALL = Style.RESET_ALL
        init()
    except:
        BRIGHTRED = BRIGHTGREEN = BRIGHTBLUE = BRIGHTYELLOW = RESET_ALL = ""
    if len(sys.argv) < 2:
        print(BRIGHTGREEN + "BURST plots wizard (version 1.0)")
        print(BRIGHTBLUE + "Usage: %s -k Key [-p PlotterPath] [-x PlotCore] [-m MaxMemUsage] [-M CreepMinerPath] "
                           "[-c Path2MiningConf] [-R] [-s MinPlotSize] [-S MaxPlotSize] [-f MinDiskFree] [-d] "
                           "[-C PlotWizardConf] [-t TMPDIR] [PLOTSDIR1] [PLOTSDIR2]..." % sys.argv[0])
        print("-p = Path to cg_obup plotter executable.")
        print("-C = Path to plotWizard's configuration file.")
        print("-c = Path to mining.conf for creepMiner.")
        print("-m = Maximum RAM usage (Default = 4 GB).")
        print("-M = Path to creepMiner executable.")
        print("-R = Restart creepMiner after each successfully created plot file.")
        print("-s = Minimum size of plot files (Default = 10 GB).")
        print("-S = Maximum size of plot files (Default = 1 TB).")
        print("-f = Minimum free size disk space to keep (Default is 0).")
        print("     Example: 4G,/media/plot1:10G -> For /media/plot1 10G, for all others 4G")
        print("-t = Temporary directory for plot file creation.")
        print("PLOTSDIRx directories for plot files." + RESET_ALL)
        sys.exit(1)
    # Read arguments
    key = None
    wizardConfPathName = None
    # Plotter args
    plotterPathName = None
    plotCore = None
    plotMemUsage = 4 * GB
    # Miner args
    minerPathName = None
    miningConfPathName = None
    bRestartMiner = False
    # Other args
    minPlotSize = 10 * GB
    maxPlotSize = MB * MB
    minDiskFree = {}
    bDryRun = False
    tmpDirName = None
    plotDirNames = set()
    for arg in sys.argv[1:]:
        if arg == "-C":
            wizardConfPathName = arg
        elif wizardConfPathName == "-C":
            wizardConfPathName = arg
            break
    if wizardConfPathName is None:
        wizardConfPathName = "wizard.conf"
    if wizardConfPathName and os.path.exists(wizardConfPathName):
        try:
            conf = json.loads(open(wizardConfPathName).read())
            key = conf["key"]
            plotterPathName = conf["plotterPathName"]
            plotCore = conf["plotCore"]
            plotMemUsage = conf["plotMemUsage"]
            minerPathName = conf["minerPathName"]
            miningConfPathName = conf["miningConfPathName"]
            bRestartMiner = conf["bRestartMiner"]
            minPlotSize = conf["minPlotSize"]
            maxPlotSize = conf["maxPlotSize"]
            minDiskFree = conf["minDiskFree"]
            tmpDirName = conf["tmpDirName"]
            plotDirNames = set(conf["plotDirNames"])
        except Exception as exc:
            print(BRIGHTRED + str(exc) + RESET_ALL)
    for arg in sys.argv[1:]:
        if arg == "-k":
            key = arg
            continue
        if key == "-k":
            key = int(arg)
            continue
        if arg == "-C":
            wizardConfPathName = arg
            continue
        if wizardConfPathName == "-C":
            wizardConfPathName = arg
            continue
        if arg == "-p":
            plotterPathName = arg
            continue
        if plotterPathName == "-p":
            if not os.path.exists(arg):
                print(BRIGHTRED + f"Error: Plotter {arg} not found!" + RESET_ALL)
                sys.exit(1)
            plotterPathName = arg
            continue
        if arg == "-x":
            plotCore = arg
            continue
        if plotCore == "-x":
            if arg not in ( "1", "2" ):
                print(BRIGHTRED + "Error: Invalid plot core! Valid values are 1 and 2." + RESET_ALL)
                sys.exit(1)
            plotCore = arg
            continue
        if arg == "-m":
            plotMemUsage = arg
            continue
        if plotMemUsage == "-m":
            plotMemUsage = arg2Int(arg, NONCE_SIZE)
            plotMemUsage -= plotMemUsage % NONCE_SIZE
            if plotMemUsage < 512 * MB:
                print(BRIGHTRED + "Error: Too small RAM usage configured (Min = 512MB)." + RESET_ALL)
                sys.exit(1)
            continue
        if arg == "-M":
            minerPathName = arg
            continue
        if minerPathName == "-M":
            if not os.path.exists(arg):
                print(BRIGHTRED + f"Error: creepMiner {arg} not found!" + RESET_ALL)
                sys.exit(1)
            minerPathName = arg
            continue
        if arg == "-c":
            miningConfPathName = arg
            continue
        if miningConfPathName == "-c":
            if not os.path.isfile(arg):
                print(BRIGHTRED + "Error: CreepMiner configuration file not found!" + RESET_ALL)
                sys.exit(1)
            miningConfPathName = arg
            continue
        if arg == "-R":
            bRestartMiner = True
            continue
        if arg == "-s":
            minPlotSize = arg
            continue
        if minPlotSize == "-s":
            minPlotSize = arg2Int(arg)
            if minPlotSize < GB:
                print(BRIGHTRED + "Error: Minimum plot size too small (Min = 1 GB)!" + RESET_ALL)
                sys.exit(1)
            continue
        if arg == "-S":
            maxPlotSize = arg
            continue
        if maxPlotSize == "-S":
            maxPlotSize = arg2Int(arg)
            if maxPlotSize < 100 * GB:
                print(BRIGHTRED + "Error: Maximum plot size too small (Min = 100 GB)!" + RESET_ALL)
                sys.exit(1)
            continue
        if arg == "-f":
            minDiskFree = arg
            continue
        if minDiskFree == "-f":
            for item in arg.split(","):
                if ":" in item:
                    pathName, value = item.split(":")
                    minFree = arg2Int(value)
                else:
                    pathName = "*"
                    minFree = arg2Int(item)
                if minFree < 0:
                    print(BRIGHTRED + "Error: Minimum disk free <0!" + RESET_ALL)
                    sys.exit(1)
                if not isinstance(minDiskFree, dict):
                    minDiskFree = {}
                minDiskFree[pathName] = minFree
            continue
        if arg == "-d":
            bDryRun = True
            continue
        if arg == "-t":
            tmpDirName = arg
            continue
        if tmpDirName == "-t":
            if not os.path.isdir(arg):
                print(BRIGHTRED + "Error: Temporary directory not found!" + RESET_ALL)
                sys.exit(1)
            tmpDirName = arg
            continue
        plotDirNames.add(arg)
    if wizardConfPathName:
        with open(wizardConfPathName, "w") as O:
            O.write(json.dumps({ "key" : key,
                                 "plotterPathName" : plotterPathName,
                                 "plotCore" : plotCore,
                                 "plotMemUsage" : plotMemUsage,
                                 "minerPathName" : minerPathName,
                                 "miningConfPathName" : miningConfPathName,
                                 "bRestartMiner" : bRestartMiner,
                                 "minPlotSize" : minPlotSize,
                                 "maxPlotSize" : maxPlotSize,
                                 "minDiskFree" : minDiskFree,
                                 "tmpDirName" : tmpDirName,
                                 "plotDirNames" : sorted(plotDirNames) }, indent = 4))
    if key is None:
        print(BRIGHTRED + "Error: Key not set!" + RESET_ALL)
        sys.exit(1)
    # Add plot directories from mining.conf
    if miningConfPathName:
        mininConf = json.loads(open(miningConfPathName, "r").read())
        plotDirNames.update(mininConf["mining"]["plots"])
    # Remove invalid plot directories
    for dirName in set(plotDirNames):
        if not os.path.isdir(dirName):
            plotDirNames.remove(dirName)
            print(BRIGHTRED + f"Warning: Plot directory {dirName} not found!")
    # Get startnonce from existing plot files
    startNonce = 0
    for dirName in plotDirNames:
        for fileName in os.listdir(dirName):
            try:
                tmpKey, tmpStartNonce, nonces, stagger = [ int(x) for x in fileName.split("_") ]
                if tmpKey == key:
                    startNonce = max(startNonce, tmpStartNonce + nonces)
            except:
                pass
    # Filter plot directories by device
    mountPoints = {}
    for dirName in sorted(plotDirNames):
        mountPoint = findMountPoint(dirName)
        if mountPoint in mountPoints:
            plotDirNames.remove(dirName)
            print(BRIGHTRED + f"Ignore {dirName} on same device as {mountPoints[mountPoint]}!" + RESET_ALL)
        else:
            mountPoints[mountPoint] = dirName
    plotDirNames = list(mountPoints.values())
    # Create plot files
    bCancel = False
    minerLock = _thread.allocate_lock()
    plotFileCnt = 0
    maxPlotSize -= maxPlotSize % plotMemUsage
    threadCnt = os.cpu_count() // 2
    plotFiles = deque()
    moveFiles = deque()
    thrCreate = Thread(target = createPlotFilesThread, args = ( plotFiles, moveFiles ), daemon = True)
    thrCreate.start()
    thrMove = Thread(target = movePlotFilesThread, args = ( moveFiles, minerLock ), daemon = True)
    thrMove.start()
    if bRestartMiner:
        thrMiner = Thread(target = minerThread, args = ( minerLock, ), daemon = True)
        thrMiner.start()
    else:
        thrMiner = None
    granularity = NONCE_SIZE * threadCnt * 8
    plotFileNonces = {}
    for dirName in plotDirNames:
        totalPlotFileSize = diskFree(dirName) - minDiskFree.get(dirName, minDiskFree["*"])
        if totalPlotFileSize < minDiskFree.get(dirName, minDiskFree["*"]):
            print(BRIGHTRED + f"Disk for {dirName} is full!" + RESET_ALL)
        else:
            plotFileNonces[dirName] = (totalPlotFileSize - totalPlotFileSize % granularity) // NONCE_SIZE
    while plotFileNonces:
        for dirName in sorted(plotFileNonces):
            if (dirName == tmpDirName) and (len(plotFileNonces) > 1):
                continue
            nonces = min(plotFileNonces[dirName], maxPlotSize // NONCE_SIZE)
            print(BRIGHTGREEN + f"Create plot file with {nonces} nonces ({nonces * NONCE_SIZE //GB} GB) for {dirName}..."
                  + RESET_ALL)
            plotFileCnt += 1
            plotFiles.append(( plotFileCnt, startNonce, nonces, dirName ))
            startNonce += nonces
            plotFileNonces[dirName] -= nonces
            if plotFileNonces[dirName] <= 0:
                del plotFileNonces[dirName]
    plotFiles.append(None)
    print(BRIGHTBLUE + "Waiting for createPlotFilesThread to finish..." + RESET_ALL)
    while not bCancel and thrCreate.is_alive():
        try:
            thrCreate.join(0.1)
        except:
            bCancel = True
    print(BRIGHTBLUE + "Waiting for movePlotFilesThread to finish..." + RESET_ALL)
    thrMove.join()
    if thrMiner:
        print(BRIGHTBLUE + "Waiting for minerThread to finish..." + RESET_ALL)
        bCancel = True
        if minerLock.locked() :
            minerLock.release()
        thrMiner.join()
    print(BRIGHTBLUE + "Finished." + RESET_ALL)
