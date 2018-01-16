#!/usr/bin/python3

import os
import _thread
from util import Semaphore, diskFree, SCOOP_SIZE, NUM_SCOOPS, NONCE_SIZE

MAX_READ = 4 * NONCE_SIZE
MB = 1024 * 1024


def addPlotFile(pathName):
    fileName = os.path.basename(pathName)
    try:
        key, startNonce, nonces, stagger = [ int(x) for x in fileName.split("_") ]
        size = os.path.getsize(pathName)
        if nonces * NONCE_SIZE != size:
            msg = f"Error: Source file has invalid size! Expected {nonces * NONCE_SIZE} but file has {size}!"
            print(BRIGHTRED + msg + RESET_ALL)
            raise Exception(msg)
        plotFiles[pathName] = ( nonces, stagger )
        plotInfos[startNonce] = [ pathName, 0 ]
        return key
    except Exception as exc:
        print(BRIGHTRED + f"Warning: Ignoring invalid source filename: {exc}" + RESET_ALL)
        raise


def plotNonces(startNonce, nonces):
    cmdLine = [ plotterPathName, "-k", str(key), "-d", tmpDirName, "-t", str(threads), "-x", plotCore,
                "-s", str(startNonce), "-n", str(nonces) ]
    print(BRIGHTGREEN + f"Compute {nonces} missing nonces through running:")
    print("  " + " ".join(cmdLine))
    if bDryRun :
        return None
    prc = subprocess.run(cmdLine, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    print(BRIGHTGREEN + prc.stdout.decode("utf-8"))
    print(BRIGHTRED + prc.stderr.decode("utf-8") + RESET_ALL)
    if prc.returncode :
        print(BRIGHTRED + f"Error: Plotter returned with error code {prc.returncode}!" + RESET_ALL)
        sys.exit(1)
    return os.path.join(tmpDirName, f"{key}_{startNonce}_{nonces}_{nonces}")


def readerThread(buf, sem, lock):
    try:
        for scoop in range(NUM_SCOOPS):
            for nr, startNonce in enumerate(startNonces):
                pathName, skip = plotInfos[startNonce]
                nonces, stagger = plotFiles[pathName]
                groupCnt = nonces // stagger
                groupSize = stagger * NONCE_SIZE
                groupScoopSize = stagger * SCOOP_SIZE
                with open(pathName, "rb") as I:
                    bytes2Read = (nonces if skip <= 0 else nonces - skip) * SCOOP_SIZE
                    for group in range(groupCnt):
                        I.seek(group * groupSize + scoop * groupScoopSize)
                        size = reading = min(groupScoopSize, bytes2Read)
                        while reading > 0:
                            if bStop:
                                raise StopIteration("Cancelled by user")
                            data = I.read(min(reading, MAX_READ))
                            buf.append(data)
                            if lock.locked():
                                lock.release()
                            sem.acquire()
                            reading -= MAX_READ
                        bytes2Read -= size
    except Exception as exc:
        print(BRIGHTRED + str(exc) + RESET_ALL)
    buf.append(None)
    if lock.locked():
        lock.release()


if __name__ == "__main__":
    import sys
    import time
    import subprocess
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
        print(BRIGHTGREEN + "BURST plots merger (version 1.0)")
        print(BRIGHTBLUE + "Usage: %s [-p PlotterPath] [-x PlotCore] [-r] [-d] [-o OUTDIR] [-t TMPDIR] INPATH1 INPATH2 ..."
              % sys.argv[0])
        print("-r = Remove old files after successfull merge.")
        print("-d = Dry run.")
        print(BRIGHTGREEN + "If OUTDIR is missing then the optimized plot is written to the same directory " 
              "as the source plots.")
        print("Unoptimized plots are also accepted as source files." + RESET_ALL)
        sys.exit(1)
    # Read arguments
    plotterPathName = None
    plotCore = None
    outDirName = None
    tmpDirName = None
    bRemoveOld = False
    bDryRun = False
    plotFiles = {}  # [path] = ( nonces, stagger ) -> information about plotfiles
    plotInfos = {}  # [startnonce] = [ path, skip ]
                    #   -> skip > 0 --> Overlapping plot files. Nonces to skip.
                    #   -> skip < 0 --> Missing nonces. Will be computed/plotted.
    key = None
    for arg in sys.argv[1:]:
        if arg == "-r":
            bRemoveOld = True
            continue
        if arg == "-d":
            bDryRun = True
            continue
        if arg == "-o":
            outDirName = arg
            continue
        if outDirName == "-o":
            if not os.path.exists(arg):
                print(BRIGHTRED + f"Error: Output directory does not exist!" + RESET_ALL)
                sys.exit(1)
            outDirName = arg
            continue
        if arg == "-t":
            tmpDirName = arg
            continue
        if tmpDirName == "-t":
            tmpDirName = arg
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
            plotCore = arg
            continue
        if not os.path.exists(arg):
            continue
        if os.path.isdir(arg):
            for fileName in os.listdir(arg):
                if "." in fileName or (fileName.count("_") != 3) :
                    continue
                if key is None or fileName.startswith(str(key) + "_") :
                    try:
                        key = addPlotFile(os.path.join(arg, fileName))
                    except:
                        pass
                else:
                    print(BRIGHTRED + f"Warning: Ignoring file {fileName} with different key!" + RESET_ALL)
        else:
            fileName = os.path.basename(arg)
            if "." in fileName or (fileName.count("_") != 3):
                continue
            if key is None or fileName.startswith(str(key) + "_"):
                try:
                    key = addPlotFile(arg)
                except:
                    pass
            else:
                print(BRIGHTRED + "Error: Tried to merge plot files with different keys!" + RESET_ALL)
                sys.exit(1)
    if len(plotFiles) < 2:
        print(BRIGHTRED + f"Error: No source plot files to merge!" + RESET_ALL)
        sys.exit(1)
    if plotCore is None:
        plotCore = "0"
    if tmpDirName in ( None, "-t" ):
        tmpDirName = os.path.dirname(list(plotFiles.keys())[0])
    # Check for overlapping files and missing nonces
    startNonces = sorted(plotInfos)
    totalNonces = 0
    bCreateMissing = False
    for nr, startNonce in enumerate(startNonces):
        pathName = plotInfos[startNonce][0]
        nonces = plotFiles[pathName][0]
        totalNonces += nonces
        if (nr == 0) and outDirName in ( None, "-o" ):
            outDirName = os.path.dirname(pathName)
        if nr + 1 >= len(startNonces):
            continue
        skip = plotInfos[startNonce][1] = startNonce + nonces - startNonces[nr + 1]
        totalNonces -= skip
        if skip > 0:
            print(BRIGHTYELLOW + f"Info: Overlapping files\n {pathName}\n and\n {plotInfos[startNonces[nr + 1]][0]}!")
            print(BRIGHTGREEN + f"Skipping last {skip} nonces in {pathName}!" + RESET_ALL)
        elif skip < 0:
            bCreateMissing = True
            print(BRIGHTYELLOW + f"Info: Missing {-skip} nonces between\n {pathName}\n and\n {plotInfos[startNonces[nr + 1]][0]}!")
            if plotterPathName is None:
                print(BRIGHTRED + "Error: Path to plotter not set!" + RESET_ALL)
                sys.exit(1)
            print(BRIGHTGREEN + f"Missing {-skip} nonces will be created." + RESET_ALL)
    # Plot missing nonces
    threads = os.cpu_count() // 2
    threads8 = threads * 8
    if bCreateMissing:
        # Compute missing nonces
        for startNonce in startNonces:
            pathName, skip = plotInfos[startNonce]
            nonces = plotFiles[pathName][0]
            if skip >= 0:
                continue
            missingNonces = -skip
            if missingNonces < threads8:
                threads = max((missingNonces + 7) // 8, 1)
                missingNonces = threads * 8
            elif missingNonces % (threads8):
                missingNonces = ((missingNonces // threads8) + 1) * threads8
            newStartNonce = startNonce + nonces
            pathName = plotNonces(newStartNonce, missingNonces)
            if pathName:
                plotFiles[pathName] = ( missingNonces, missingNonces )
                plotInfos[newStartNonce] = [ pathName, missingNonces + skip ]
        startNonces = sorted(plotInfos)
    if totalNonces % threads8:
        plotInfos[startNonces[-1]][1] = totalNonces % threads8
        newStartNonce = (totalNonces // threads8) * threads8
        totalNonces = newStartNonce + threads8
        pathName = plotNonces(newStartNonce, threads8)
        if pathName:
            plotFiles[pathName] = ( threads8, threads8 )
            plotInfos[newStartNonce] = [ pathName, 0 ]
    startNonces = sorted(plotInfos)
    outPathName = os.path.join(outDirName, f"{key}_{startNonces[0]}_{totalNonces}_{totalNonces}")
    outSize = totalNonces * NONCE_SIZE
    print(BRIGHTYELLOW + f"Merging {len(plotInfos)} plot files:")
    for startNonce in startNonces:
        print("  " + plotInfos[startNonce][0])
    print(f"Destination file {outPathName} will have:")
    print(f"  Start Nonce: {startNonces[0]}")
    print(f"  Nonces:      {totalNonces}")
    print(f"  File size:   {outSize // 1024 // MB} GB")
    for pathName in ( outPathName + ".merging", outPathName ):
        if os.path.exists(pathName):
            print(BRIGHTRED + f"Warning: Destination file {pathName} already exists! Removing it!" + RESET_ALL)
            if not bDryRun:
                os.remove(pathName)
    if diskFree(outDirName) < outSize:
        print(BRIGHTRED + "Error: Not enough free space on disk for merged plot file!" + RESET_ALL)
        sys.exit(1)
    print(BRIGHTGREEN + f"Writing merged plot to {outPathName}.merging..." + RESET_ALL)
    if bDryRun:
        print(BRIGHTBLUE + "Skip merging plot files because of dry run option.")
        sys.exit(0)
    bStop = False
    buf = deque()
    sem = Semaphore(1000)
    lock = _thread.allocate_lock()
    thrReader = Thread(target = readerThread, args = ( buf, sem, lock ), daemon = True)
    thrReader.start()
    cnt = written = lastWritten = 0
    t0 = t1 = time.time()
    with open(outPathName + ".merging", "wb") as O:
        while thrReader.is_alive() or buf:
            try:
                data = buf.popleft()
                if data is None:
                    break
                O.write(data)
                sem.release()
                cnt += 1
                written += len(data)
                if cnt >= 1000:
                    t2 = time.time()
                    print("%.1f%% written. %d MB/s. " % (100 * written / outSize, (written - lastWritten) // MB / (t2 - t1)),
                          end = "\r")
                    cnt = 0
                    lastWritten = written
                    t1 = t2
            except KeyboardInterrupt:
                bStop = True
                buf.clear()
            except:
                lock.acquire()
    thrReader.join()
    if bStop:
        print(BRIGHTRED + "\nCancelled by user")
        sys.exit(1)
    os.rename(outPathName + ".merging", outPathName)
    if bRemoveOld:
        print(BRIGHTBLUE + "Removing old plot files...")
        for pathName in plotFiles:
            try:
                os.remove(pathName)
            except Exception as exc:
                # Windows can be a big pile of shit in some situations. One of them is the file locking...
                print(BRIGHTRED + f"Error: Failed removing plot file {pathName}:\n{exc}" + RESET_ALL)
    print(BRIGHTGREEN + f"Finished after {int(time.time() - t0)} seconds.")
