#!/usr/bin/python3

import os
import _thread

SCOOP_SIZE = 64
NUM_SCOOPS = 4096
NONCE_SIZE = NUM_SCOOPS * SCOOP_SIZE
MAX_READ = 4 * NONCE_SIZE


# Create own semaphore class which is much faster than the original version in the
# threading module.
class Semaphore(object):

    def __init__(self, value):
        self._value = value
        self._value_lock = _thread.allocate_lock()
        self._zero_lock = _thread.allocate_lock()
        self._zero_lock.acquire()

    def acquire(self):
        if self._value < 1:
            self._zero_lock.acquire()
        with self._value_lock:
            self._value -= 1

    def release(self):
        if self._zero_lock.locked():
            try:
                self._zero_lock.release()
            except:
                pass
        with self._value_lock:
            self._value += 1


def diskFree(pathName):
    if hasattr(os, 'statvfs') :  # POSIX
        st = os.statvfs(pathName)
        return st.f_bavail * st.f_frsize
    if os.name == 'nt' :  # Windows
        import ctypes
        import sys
        free = ctypes.c_ulonglong()
        ret = ctypes.windll.kernel32.GetDiskFreeSpaceExW(pathName, ctypes.byref(free), None, None)
        if ret == 0 :
            raise ctypes.WinError()
        return free.value
        raise NotImplementedError("platform not supported")


def addPlotFile(pathName):
    fileName = os.path.basename(pathName)
    try:
        key, startnonce, nonces, stagger = [ int(x) for x in fileName.split("_") ]
        size = os.path.getsize(pathName)
        if nonces * NONCE_SIZE != size:
            msg = f"Error: Source file has invalid size! Expected {nonces * NONCE_SIZE} but file has {size}!"
            print(BRIGHTRED + msg + RESET_ALL)
            raise Exception(msg)
        plotFiles[pathName] = ( nonces, stagger )
        plotInfos[startnonce] = [ pathName, nonces, 0 ]
        return key
    except Exception as exc:
        print(BRIGHTRED + f"Warning: Ignoring invalid source filename: {exc}" + RESET_ALL)
        raise


def readerThread(buf, sem, lock):
    try:
        for startnonce in startnonces:
            pathName, nonces2Read, _ = plotInfos[startnonce]
            # TODO: Consider case nonces2Read < nonces -> Skip last (overlapping) nonces
            nonces, stagger = plotFiles[pathName]
            groupCnt = nonces // stagger
            groupSize = stagger * NONCE_SIZE
            groupScoopSize = stagger * SCOOP_SIZE
            with open(pathName, "rb") as I:
                for scoop in range(NUM_SCOOPS):
                    for group in range(groupCnt):
                        I.seek(group * groupSize + scoop * groupScoopSize)
                        size = groupScoopSize
                        while size > 0:
                            buf.append(I.read(size if size < MAX_READ else MAX_READ))
                            if lock.locked():
                                lock.release()
                            sem.acquire()
                            size -= MAX_READ
    except Exception as exc:
        print(exc)


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
        print(BRIGHTBLUE + "Usage: %s [-p PlotterPath] [-x PlotCore] [-d] [-o OUTDIR] INPATH1 INPATH2 ..." % sys.argv[0])
        print("-d = Delete old files after successfull merge.")
        print(BRIGHTGREEN + "If OUTDIR is missing then the optimized plot is written to the same directory " 
              "as the source plots.")
        print("Unoptimized plots are optimized." + RESET_ALL)
        sys.exit(1)
    # Read arguments
    plotterPathName = None
    plotCore = None
    outDirName = None
    bDeleteOld = False
    plotFiles = {}  # [path] = ( nonces, stagger ) -> information about plotfiles
    plotInfos = {}  # [startnonce] = [ path, nonces_to_read, hole_size ]
                    #   -> nonces to read from file (<nonces in case of overlapping files)
    key = None
    for arg in sys.argv[1:]:
        if arg == "-d":
            bDeleteOld = True
            continue
        if arg == "-o":
            outDirName = arg
            continue
        if outDirName == "-o":
            outDirName = arg
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
    if plotCore is None:
        plotCore = "0"
    # Check for overlapping files and missing nonces
    startnonces = sorted(plotInfos)
    totalnonces = 0
    bCreateMissing = False
    for nr, startnonce in enumerate(startnonces):
        pathName, nonces, _ = plotInfos[startnonce]
        totalnonces += nonces
        if (nr == 0) and outDirName in ( None, "-o" ):
            outDirName = os.path.dirname(pathName)
        if nr + 1 >= len(startnonces):
            continue
        skip = startnonce + nonces - startnonces[nr + 1]
        totalnonces -= skip
        if skip > 0:
            plotInfos[ startnonce ][1] -= skip
            print(BRIGHTYELLOW + f"Info: Overlapping files\n {pathName}\n and\n {plotInfos[startnonces[nr + 1][0]]}!")
            print(BRIGHTGREEN + f"Skipping last {skip} nonces in {pathName}!" + RESET_ALL)
        elif skip < 0:
            plotInfos[startnonce][2] = -skip
            bCreateMissing = True
            print(BRIGHTYELLOW + f"Info: Missing {-skip} nonces between\n {pathName}\n and\n {plotInfos[startnonces[nr + 1]][0]}!")
            if plotterPathName is None:
                print(BRIGHTRED + "Error: Path to plotter not set!" + RESET_ALL)
                sys.exit(1)
            print(BRIGHTGREEN + f"Missing {-skip} nonces will be created." + RESET_ALL)
    outPathName = os.path.join(outDirName, f"{key}_{startnonces[0]}_{totalnonces}_{totalnonces}")
    outSize = totalnonces * NONCE_SIZE
    print(BRIGHTYELLOW + f"Merging {len(plotInfos)} plot files:")
    for startnonce in startnonces:
        print("  " + plotInfos[startnonce][0])
    print(f"Destination file {outPathName} will have:")
    print(f"  Start Nonce: {startnonces[0]}")
    print(f"  Nonces:      {totalnonces}")
    print(f"  File size:   {outSize // 1024 // 1024 // 1024} GB")
    if bCreateMissing:
        # Compute missing nonces
        for startnonce in startnonces:
            pathName, nonces, holeSize = plotInfos[startnonce]
            if holeSize <= 0:
                continue
            cmdLine = [ plotterPathName, "-k", str(key), "-d", outDirName, "-t", str(os.cpu_count()), "-x", plotCore,
                        "-s", str(startnonce), "-n", str(holeSize) ]
            print(BRIGHTGREEN + f"Compute {holeSize} missing nonces through running:")
            print("  " + "".join(cmdLine))
            prc = subprocess.run(cmdLine, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
            print(prc.stdout)
            print(prc.stderr)
            if prc.returncode:
                print(BRIGHTRED + f"Error: Plotter returned with error code {prc.returncode}!" + RESET_ALL)
                sys.exit(1)
            pathName = f"{key}_{startnonce}_{holeSize}_{holeSize}"
            plotFiles[pathName] = ( holeSize, holeSize )
            plotInfos[startnonce] = [ pathName, holeSize, 0 ]
        startnonces = sorted(plotInfos)
    if os.path.exists(outPathName):
        print(BRIGHTRED + f"Warning: Destination file {outPathName} already exists! Removing it!" + RESET_ALL)
        os.remove(outPathName)
    if diskFree(outDirName) < outSize:
        print(BRIGHTRED + "Error: Not enough free space on disk for merged plot file!" + RESET_ALL)
        sys.exit(1)
    print(BRIGHTGREEN + f"Writing merged plot to {outPathName}.merging..." + RESET_ALL)
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
                O.write(data)
                sem.release()
                cnt += 1
                written += len(data)
                if cnt >= 1000:
                    t2 = time.time()
                    print("%.1f%% written. %d MB/s. " % (100 * written / outSize, (written - lastWritten) / (t2 - t1)),
                          end = "\r")
                    cnt = 0
                    lastWritten = written
                    t1 = t2
            except:
                lock.acquire()
    print(BRIGHTGREEN + f"Finished after {int(time.time() - t0)} seconds.")
