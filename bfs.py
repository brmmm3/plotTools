#!/usr/bin/python3

import os
import sys
import struct
import time
import glob
import traceback
from collections import deque
from threading import Thread, Event

SECTOR_SIZE = 512
NONCE_SIZE = 262144

MB = 1024 * 1024
GB = 1024 * MB

ST_OK = 1
ST_INCOMPLETE = 2


def writerThread(F, q, ev):
    while True:
        try:
            data = q.popleft()
        except:
            ev.wait()
            ev.clear()
            continue
        if data is None:
            break
        F.write(data)


def copyFile(S, D, size):
    q = deque()
    ev = Event()
    thr = Thread(target=writerThread, args=(D, q, ev), daemon=True)
    thr.start()
    t0 = time.time()
    cnt = 0
    total = 0
    while True:
        data = S.read(MB)
        if not data:
            break
        dataLen = len(data)
        if total + dataLen < size:
            q.append(data)
            ev.set()
            total += dataLen
            cnt += dataLen
        else:
            q.append(data[:size - total])
            ev.set()
            break
        t1 = time.time()
        dt = t1 - t0
        if dt >= 2.0:
            speed = cnt // dt
            leftSize = size - total
            minutes = leftSize / speed / 60 # Time left in minutes
            hours = int(minutes / 60)
            minutes = int(minutes) - hours * 60
            print(f"\rCopied {total / GB:.1f} GB ({speed // MB} MB/s). Left {leftSize / GB:.1f} GB ({hours:02d}:{minutes:02d})     ", end="")
            cnt = 0
            t0 = t1
        while len(q) > 64:
            time.sleep(0.01)
    q.append(None)
    thr.join()
    print()


def getDiskSize(dev):
    return int(open(f"/sys/block/{os.path.basename(dev)}/size").read()) * SECTOR_SIZE


def initDevice(dev):
    print("Initialize device...")
    with open(dev, "wb") as D:
        D.write(b"BFS0" + b"\0" * 1020)


def readTOC(dev):
    with open(dev, "rb") as D:
        tocData = bytearray(D.read(1024))
    if not tocData.startswith(b"BFS0"):
        raise Exception("ERROR: Device does not have a BFS table!")
    toc = {}
    for i in range(31):
        pos = 4 + i * 32
        key, startNonce, nonces, stagger, info = struct.unpack("<QQIIQ", tocData[pos:pos + 32])
        if key != 0:
            if stagger > 0:
                fileName = f"{key}_{startNonce}_{nonces}_{stagger}"
            else:
                fileName = f"{key}_{startNonce}_{nonces}"
            toc[info & 0xffffffffffff] = (key, startNonce, nonces, stagger, info >> 48, fileName)
    return tocData, toc


def listPlotFiles(dev):
    if "*" in dev or "?" in dev:
        devices = glob.glob(dev)
    else:
        devices = (dev, )
    for device in devices:
        try:
            size = getDiskSize(device) - 2 * SECTOR_SIZE
            print(f"Contents of {device} with size {int(size / GB + 0.5)} GB:")
            for startPos, (key, startNonce, nonces, stagger, status, fileName) in sorted(readTOC(device)[1].items()):
                size -= nonces * NONCE_SIZE
                print(
                    f"{key}_{startNonce}_{nonces}_{stagger} with size {nonces // 4096}GB starts at sector {startPos >> 9}")
            print(f"{int(size / GB + 0.5)} GB ({size // NONCE_SIZE} Nonces) free space left.")
        except Exception as exc:
            print(exc)
            continue


def writePlotFiles(dev, plotFiles):
    size = getDiskSize(dev) - 2 * SECTOR_SIZE
    tocData, toc = readTOC(dev)
    # Compute free blocks
    usedBlocks = {}
    for startPos, (key, startNonce, nonces, stagger, status, fileName) in toc.items():
        usedBlocks[startPos] = nonces * NONCE_SIZE
    freeBlocks = {}
    if usedBlocks:
        startPos = min(usedBlocks)
        lastPos = startPos + usedBlocks[startPos]
        for startPos, blockSize in sorted(usedBlocks.items())[1:]:
            if lastPos < startPos:
                freeBlocks[lastPos] = startPos - lastPos
            lastPos = startPos + blockSize
        if lastPos < size:
            freeBlocks[lastPos] = size - lastPos
    else:
        freeBlocks[1024] = size
    # Write plot files
    with open(dev, "wb") as D:
        for plotFile in plotFiles:
            plotFileName = os.path.basename(plotFile)
            # Check filename
            try:
                key, startNonce, nonces, stagger = [ int(x) for x in os.path.basename(plotFileName).split("_") ]
            except Exception as exc:
                try:
                    key, startNonce, nonces = [int(x) for x in os.path.basename(plotFileName).split("_")]
                    stagger = 0
                except Exception as exc:
                    print(f"ERROR: Invalid source filename: {plotFile}:\n{exc}")
                    continue
            # Check if TOC is full
            if len(toc) >= 31:
                raise Exception("ERROR: TOC is full!")
            bExists = False
            for tmpKey, tmpStartNonce, tmpNonces, tmpStagger, tmpStatus, fileName in toc.values():
                if (tmpKey, tmpStartNonce, tmpNonces) == (key, startNonce, nonces):
                    if tmpStagger == stagger:
                        print(f"ERROR: File {fileName} already exists!")
                    else:
                        print(f"ERROR: File {fileName} already exists with different stagger size!")
                    bExists = True
                    break
            if bExists:
                continue
            # Search for free block with enough size
            plotSize = os.stat(plotFile).st_size
            for startPos in sorted(freeBlocks):
                if freeBlocks[startPos] >= plotSize:
                    break
            else:
                print(f"ERROR: Not enough free space for {plotFile}!")
                continue
            print(f"Write file {plotFile} to {dev}...")
            # Copy file
            t0 = time.time()
            D.seek(startPos)
            with open(plotFile, "rb") as F:
                copyFile(F, D, plotSize)
            if freeBlocks[startPos] > plotSize:
                freeBlocks[startPos + plotSize] = freeBlocks[startPos] - plotSize
            del freeBlocks[startPos]
            # Add file to TOC
            toc[startPos] = (key, startNonce, nonces, stagger, ST_OK, f"{key}_{startNonce}_{nonces}_{stagger}")
            for i in range(31):
                pos = 4 + i * 32
                if struct.unpack("<QQIIQ", tocData[pos:pos + 32])[0] == 0:
                    info = (ST_OK << 48) | startPos
                    tocData[pos:pos + 32] = struct.pack("<QQIIQ", key, startNonce, nonces, stagger, info)
                    D.seek(0)
                    D.write(tocData)
                    break
            print(f"Written after {int(time.time() - t0)} seconds.")


def readPlotFiles(dev, plotFiles):
    for startPos, ( key, startNonce, nonces, stagger, status, fileName ) in sorted(readTOC(dev)[1].items()):
        for plotFile in plotFiles:
            if plotFile.endswith(fileName):
                print(f"Copy file {os.path.basename(plotFile)} from {dev} to {plotFile}...")
                t0 = time.time()
                with open(plotFile, "wb") as F:
                    with open(dev, "rb") as D:
                        D.seek(startPos)
                        copyFile(D, F, nonces * NONCE_SIZE)
                print(f"Read after {int(time.time() - t0)} seconds.")
                break
        else:
            print(f"ERROR: File {fileName} not found on device {dev}!")


def deletePlotFiles(dev, plotFiles):
    toc = readTOC(dev)[1]
    for plotFile in plotFiles:
        plotFileName = os.path.basename(plotFile)
        for startPos, (key, startNonce, nonces, stagger, status, fileName) in list(toc.items()):
            if fileName == plotFileName:
                del toc[startPos]
                break
        else:
            print(f"ERROR: File {plotFileName} not found on device {dev}!")
    newData = b"BFS0" + b"\0" * 1020
    for i, startPos, (key, startNonce, nonces, stagger, status, fileName) in enumerate(sorted(toc.items())):
        pos = 4 * i * 32
        newData[pos:pos + 32] = struct.pack("<QQIIQ", key, startNonce, nonces, stagger, (status << 48) | startPos)
    # Write TOC
    with open(dev, "wb") as D:
        D.write(newData)


if __name__ == "__main__":
    command = sys.argv[1].lower()
    dev = sys.argv[2]
    if not dev.startswith("/dev/"):
        print("Parameter must be a valid disk device!")
        sys.exit(1)
    t0 = time.time()
    if command == "i":
        if input("Really want to delete all data on disk (y/n)?").lower() == "y":
            initDevice(dev)
    elif command == "l":
        listPlotFiles(dev)
    elif command == "w":
        writePlotFiles(dev, sys.argv[3:])
    elif command == "r":
        readPlotFiles(dev, sys.argv[3:])
    elif command == "d":
        deletePlotFiles(dev, sys.argv[3:])
    print(f"Finished after {int(time.time() - t0)} seconds.")
