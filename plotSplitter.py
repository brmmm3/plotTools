#!/usr/bin/python3

import os
import _thread
from util import Semaphore, diskFree, SCOOP_SIZE, NUM_SCOOPS, NONCE_SIZE

MAX_READ = 4 * NONCE_SIZE
MB = 1024 * 1024


def readerThread(buf, sem, lock):
    groupCnt = nonces // stagger
    groupSize = stagger * NONCE_SIZE
    groupScoopSize = stagger * SCOOP_SIZE
    try:
        with open(inPathName, "rb") as I:
            for scoop in range(NUM_SCOOPS):
                for group in range(groupCnt):
                    I.seek(group * groupSize + scoop * groupScoopSize)
                    reading = groupScoopSize
                    while reading > 0:
                        if bStop :
                            raise StopIteration("Cancelled by user")
                        buf.append(I.read(reading if reading < MAX_READ else MAX_READ))
                        if lock.locked():
                            lock.release()
                        sem.acquire()
                        reading -= MAX_READ
    except Exception as exc:
        print(BRIGHTRED + str(exc) + RESET_ALL)
    buf.append(None)
    if lock.locked() :
        lock.release()


if __name__ == "__main__":
    import sys
    import time
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
        print(BRIGHTGREEN + "BURST plot splitter (version 1.0)")
        print(BRIGHTBLUE + "Usage: %s [-r] [-t] [-d] [-s size] [-o OUTDIR] INPATH" % sys.argv[0])
        print("-r = Remove old files after successfull merge.")
        print("-d = Dry run.")
        print("-t = Truncate plot file instead of splitting it.")
        print("-s = Destination size.")
        print(BRIGHTGREEN + "If OUTDIR is missing then the optimized plot is written to the same directory " 
              "as the source plot.")
        sys.exit(1)
    # Read arguments
    outDirName = None
    bTruncate = False
    bRemoveOld = False
    bDryRun = False
    inPathName = None
    inSize = 0
    splitNonces = None
    for arg in sys.argv[1:]:
        if arg == "-r":
            bRemoveOld = True
            continue
        if arg == "-t":
            bTruncate = True
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
        if arg == "-s":
            splitNonces = arg
            continue
        if splitNonces == "-s":
            for c, m in ( ( "k", 1024 ), ( "m", MB ), ( "g", 1024 * MB ), ( "t", MB * MB ) ):
                if arg.endswith(c) or arg.endswith(c.upper()):
                    splitNonces = max(int(arg[:-1]) * m // NONCE_SIZE, 1)
                    break
            else:
                splitNonces = int(arg)
            continue
        if not os.path.exists(arg):
            continue
        fileName = os.path.basename(arg)
        if "." in fileName or (fileName.count("_") != 3):
            continue
        try :
            key, startNonce, nonces, stagger = [ int(x) for x in fileName.split("_") ]
            inSize = os.path.getsize(arg)
            if nonces * NONCE_SIZE != inSize :
                print(BRIGHTRED + f"Error: Source file has invalid size! Expected {nonces * NONCE_SIZE} but file has {inSize}!"
                      + RESET_ALL)
                sys.exit(1)
            inPathName = arg
        except Exception as exc :
            print(BRIGHTRED + f"Warning: Ignoring invalid source filename: {exc}" + RESET_ALL)
            sys.exit(1)
    if inPathName is None or not os.path.exists(inPathName):
        print(BRIGHTRED + f"Error: Source plot file is missing!" + RESET_ALL)
        sys.exit(1)
    if splitNonces >= nonces:
        print(BRIGHTRED + f"Error: Source plot file is smaller than split size!" + RESET_ALL)
        sys.exit(1)
    if outDirName in ( None, "-o" ):
        outDirName = os.path.dirname(inPathName)
    outFiles = []
    curNonce = startNonce
    remNonces = nonces
    while remNonces > 0:
        outPathName = os.path.join(outDirName, f"{key}_{curNonce}_{splitNonces}_{splitNonces}")
        outFiles.append([ outPathName, None, curNonce, splitNonces, splitNonces * NONCE_SIZE ])
        curNonce += splitNonces
        remNonces -= splitNonces
        if remNonces < splitNonces:
            splitNonces = remNonces
        if bTruncate and len(outFiles):
            continue
        if os.path.exists(outPathName):
            print(BRIGHTRED + f"Warning: Destination file {outPathName} already exists! Removing it!" + RESET_ALL)
            if not bDryRun:
                os.remove(outPathName)
    if diskFree(outDirName) <= os.path.getsize(inPathName):
        print(BRIGHTRED + "Error: Not enough free space on disk for merged plot file!" + RESET_ALL)
        sys.exit(1)
    bStop = False
    if not bDryRun:
        buf = deque()
        sem = Semaphore(1000)
        lock = _thread.allocate_lock()
        thrReader = Thread(target = readerThread, args = ( buf, sem, lock ), daemon = True)
        thrReader.start()
    for nr, outFile in enumerate(outFiles):
        if bTruncate and nr:
            continue
        outPathName, _, startNonce, splitNonces, outSize = outFile
        print(f"Destination file(s) {outPathName} will have:")
        print(f"  Nonces:      {splitNonces}")
        print(f"  File size:   {outSize // 1024 // MB} GB")
        outFiles[nr][1] = open(outPathName, "wb")
    if bDryRun:
        sys.exit(0)
    t0 = time.time()
    curOutFileNr = 0
    O = outFiles[0][1]
    blockSize = outFiles[0][3] * SCOOP_SIZE
    cnt = written = lastWritten = 0
    t1 = time.time()
    while thrReader.is_alive() or buf:
        try:
            data = buf.popleft()
            if data is None:
                break
            dataLen = len(data)
            if dataLen <= blockSize:
                if not O is None:
                    O.write(data)
                blockSize -= dataLen
            else:
                if not O is None:
                    O.write(data[:blockSize])
                blockSize -= dataLen
            while blockSize <= 0:
                curOutFileNr = (curOutFileNr + 1) % len(outFiles)
                O = outFiles[curOutFileNr][1]
                newBlockSize = outFiles[curOutFileNr][3] * SCOOP_SIZE
                if (blockSize < 0) and not O is None:
                    O.write(data[blockSize:dataLen + blockSize + newBlockSize])
                blockSize += newBlockSize
            sem.release()
            cnt += 1
            written += dataLen
            if cnt >= 1000:
                t2 = time.time()
                print("%.1f%% written. %d MB/s. " % (100 * written / inSize, (written - lastWritten) // MB / (t2 - t1)),
                      end = "\r")
                cnt = 0
                lastWritten = written
                t1 = t2
        except KeyboardInterrupt:
            bStop = True
            buf.clear()
        except:
            lock.acquire()
    for outFile in outFiles:
        if not outFile[1] and None:
            outFile[1].close()
    if bStop:
        print(BRIGHTRED + "\nCancelled by user")
        sys.exit(1)
    if bRemoveOld:
        print(BRIGHTBLUE + "Removing old plot file...")
        try:
            os.remove(inPathName)
        except Exception as exc:
            # Windows can be a big pile of shit in some situations. One of them is the file locking...
            print(BRIGHTRED + f"Error: Failed removing plot file {inPathName}:\n{exc}" + RESET_ALL)
    print(BRIGHTGREEN + f"Finished after {int(time.time() - t0)} seconds.")
