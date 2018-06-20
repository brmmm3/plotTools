#!/usr/bin/python3

import os
import sys
import struct
import time
import glob
from collections import deque
from threading import Thread, Event, Lock

"""
Contents TOC:
A table of 31 slots for plot files.
Each slot contains: key (64bit), startNonce (64bit), nonces(32bit), stagger(32bit), info(64bit)
If stagger=0 -> POC2 file
info: Lower 48 bits start position on disk in bytes
      Bit 48-50: 1=File is ready to use
                 2=File is incomplete (writing or plotting)
                 3=Converting to POC2
      Bits 51-63: Last written scoop when converting

TODO:
    Also write last position when writing/plotting file (Granularity is 4096 positions) for resuming
"""

VERSION = "1.0.0"

SECTOR_SIZE = 512
SHABAL256_HASH_SIZE = 32
SCOOP_SIZE = SHABAL256_HASH_SIZE * 2
SCOOPS_IN_NONCE = 4096
SCOOPS_IN_NONCE05 = 2048
NONCE_SIZE = 262144

MB = 1024 * 1024
GB = 1024 * MB

ST_OK = 1
ST_INCOMPLETE = 2
ST_CONVERTING = 3


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


def getDeviceList():
    name2dev = {}
    dev2name = {}
    for fileName in os.listdir("/dev/disk/by-id"):
        pathName = "/dev/disk/by-id/" + fileName
        if os.path.islink(pathName):
            realPathName = os.path.realpath(pathName)
            name2dev[fileName] = realPathName
            dev2name[realPathName] = fileName
    for fileName in os.listdir("/dev/disk/by-uuid"):
        pathName = "/dev/disk/by-uuid/" + fileName
        name2dev[fileName] = os.path.realpath(pathName)
    return name2dev, dev2name


def getDevices(dev):
    if dev is None:
        return glob.glob("/dev/sd?")
    if "*" in dev or "?" in dev:
        return glob.glob(dev)
    return dev,


def getDiskSize(dev):
    return int(open(f"/sys/block/{os.path.basename(dev)}/size").read()) * SECTOR_SIZE


def initDevice(dev):
    print("Initialize device...")
    with open(dev, "wb") as D:
        D.write(b"BFS0" + b"\0" * 1020)


def hasTOC(dev):
    try:
        with open(dev, "rb") as D:
            return bytearray(D.read(1024)).startswith(b"BFS0")
    except OSError:
        return False


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
            toc[info & 0xffffffffffff] = (key, startNonce, nonces, stagger, info >> 48, fileName, pos)
    return tocData, toc


def shufflePoc1ToPoc2(S, sStartPos, D, dStartPos, nonces, tocPos=0, tocData=None):
    startScoop = 0
    if tocData:
        info = struct.unpack("<Q", tocData[tocPos + 24:tocPos + 32])[0]
        status = (info >> 48) & 3
        if status == ST_INCOMPLETE:
            raise Exception("Unable to convert incomplete file!")
        if status == ST_CONVERTING:
            startScoop = info >> 51
        tocData[tocPos + 24:tocPos + 32] = struct.pack("<Q", (startScoop << 51) | (ST_CONVERTING << 48) | dStartPos)
        D.seek(0)
        D.write(tocData)
    fileSize = nonces * NONCE_SIZE
    blockSize = nonces * SCOOP_SIZE
    buf1 = bytearray(blockSize)
    buf2 = bytearray(blockSize)
    for scoop in range(startScoop, SCOOPS_IN_NONCE05):
        pos = scoop * blockSize
        S.seek(sStartPos + pos)
        cnt1 = S.readinto(buf1)
        if cnt1 != blockSize:
            raise Exception(f"Read {len(buf1)} bytes instead of {blockSize}!")
        S.seek(sStartPos + fileSize - (pos + blockSize))
        cnt2 = S.readinto(buf2)
        if cnt2 != blockSize:
            raise Exception(f"Read {len(buf2)} bytes instead of {blockSize}!")
        print(f"Converting {scoop}/{SCOOPS_IN_NONCE05 - scoop} ", end="\r")
        for off in range(32, blockSize, SCOOP_SIZE):
            tmp = buf1[off:off + SHABAL256_HASH_SIZE]
            buf1[off:off + SHABAL256_HASH_SIZE] = buf2[off:off + SHABAL256_HASH_SIZE]
            buf2[off:off + SHABAL256_HASH_SIZE] = tmp
        D.seek(dStartPos + fileSize - (pos + blockSize))
        D.write(buf2)
        D.seek(dStartPos + pos)
        D.write(buf1)
        if tocData:
            tocData[tocPos + 24:tocPos + 32] = struct.pack("<Q", (scoop << 51) | (ST_CONVERTING << 48) | dStartPos)
            D.seek(0)
            D.write(tocData)


def convertPlotFiles(dev):
    for device in getDevices(dev):
        try:
            size = getDiskSize(device) - 2 * SECTOR_SIZE
            tocData, toc = readTOC(device)
        except:
            continue
        try:
            print(f"Convert contents of {device} ({dev2name.get(device)}) with size {int(size / GB + 0.5)} GB to POC2:")
            for startPos, (_, _, nonces, stagger, _, fileName, pos) in sorted(toc.items()):
                if stagger > 0:
                    print(
                        f"POC1 {fileName} with size {nonces // 4096}GB starts at sector {startPos >> 9}")
                    print("Converting...")
                    with open(device, "rb+") as D:
                        shufflePoc1ToPoc2(D, startPos, D, startPos, nonces, pos, tocData)
                    tocData[pos + 20:pos + 32] = struct.pack("<IQ", 0, (ST_OK << 48) | startPos)
                    D.seek(0)
                    D.write(tocData)
                else:
                    print(
                        f"POC2 {fileName} with size {nonces // 4096}GB starts at sector {startPos >> 9}")
        except Exception as exc:
            print(exc)


def listPlotFiles(dev, dev2name, bVerbose):
    for device in getDevices(dev):
        try:
            size = getDiskSize(device) - 2 * SECTOR_SIZE
            _, toc = readTOC(device)
            print(f"Contents of {device} ({dev2name.get(device)}) with size {int(size / GB + 0.5)} GB:")
            for startPos, (key, startNonce, nonces, stagger, info, fileName, _) in sorted(toc.items()):
                size -= nonces * NONCE_SIZE
                status = info & 3
                if stagger > 0:
                    fileName = f"{key}_{startNonce}_{nonces}_{stagger}"
                else:
                    fileName = f"{key}_{startNonce}_{nonces}"
                if status == ST_INCOMPLETE:
                    fileName += ".plotting"
                elif status == ST_CONVERTING:
                    fileName += f".converting ({info >> 2}/{SCOOPS_IN_NONCE >> 1})"
                print(f"POC2 {fileName} with size {nonces // 4096}GB starts at sector {startPos >> 9}")
            print(f"{int(size / GB + 0.5)} GB ({size // NONCE_SIZE} Nonces) free space left.")
        except Exception as exc:
            if bVerbose:
                print(exc)


def writePlotFiles(dev, plotFiles, bPOC2):
    size = getDiskSize(dev) - 2 * SECTOR_SIZE
    tocData, toc = readTOC(dev)
    # Compute free blocks
    usedBlocks = {}
    for startPos, (_, _, nonces, _, _, _, _) in toc.items():
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
                key, startNonce, nonces, stagger = [int(x) for x in os.path.basename(plotFileName).split("_")]
            except Exception as exc:
                try:
                    # PoC2 format
                    key, startNonce, nonces = [int(x) for x in os.path.basename(plotFileName).split("_")]
                    stagger = 0
                except Exception as exc:
                    print(f"ERROR: Invalid source filename: {plotFile}:\n{exc}")
                    continue
            # Check if TOC is full
            if len(toc) >= 31:
                raise Exception("ERROR: TOC is full!")
            bExists = False
            for tmpKey, tmpStartNonce, tmpNonces, tmpStagger, _, fileName, _ in toc.values():
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
            # Find free slot in TOC
            pos = 0
            for i in range(31):
                pos = 4 + i * 32
                if struct.unpack("<QQIIQ", tocData[pos:pos + 32])[0] == 0:
                    info = (ST_INCOMPLETE << 48) | startPos
                    if bPOC2 or (stagger == 0):
                        toc[startPos] = (key, startNonce, nonces, stagger, ST_INCOMPLETE,
                                         f"{key}_{startNonce}_{nonces}", pos)
                        tocData[pos:pos + 32] = struct.pack("<QQIIQ", key, startNonce, nonces, 0, info)
                    else:
                        toc[startPos] = (key, startNonce, nonces, stagger, ST_INCOMPLETE,
                                         f"{key}_{startNonce}_{nonces}_{stagger}", pos)
                        tocData[pos:pos + 32] = struct.pack("<QQIIQ", key, startNonce, nonces, stagger, info)
                    D.seek(0)
                    D.write(tocData)
                    break
            # Copy file
            t0 = time.time()
            if bPOC2 and (stagger > 0):
                with open(plotFile, "rb") as F:
                    shufflePoc1ToPoc2(F, 0, D, startPos, nonces)
            else:
                D.seek(startPos)
                with open(plotFile, "rb") as F:
                    copyFile(F, D, plotSize)
            if freeBlocks[startPos] > plotSize:
                freeBlocks[startPos + plotSize] = freeBlocks[startPos] - plotSize
            del freeBlocks[startPos]
            # Add file to TOC
            info = (ST_OK << 48) | startPos
            if bPOC2 or (stagger == 0):
                toc[startPos] = (key, startNonce, nonces, stagger, ST_OK, f"{key}_{startNonce}_{nonces}", pos)
                tocData[pos:pos + 32] = struct.pack("<QQIIQ", key, startNonce, nonces, 0, info)
            else:
                toc[startPos] = (key, startNonce, nonces, stagger, ST_OK, f"{key}_{startNonce}_{nonces}_{stagger}", pos)
                tocData[pos:pos + 32] = struct.pack("<QQIIQ", key, startNonce, nonces, stagger, info)
            D.seek(0)
            D.write(tocData)
            print(f"Written after {int(time.time() - t0)} seconds.")


def readPlotFiles(dev, plotFiles):
    for startPos, (_, _, nonces, _, _, fileName, _) in sorted(readTOC(dev)[1].items()):
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
        for startPos, (key, startNonce, nonces, stagger, status, fileName, _) in list(toc.items()):
            if fileName == plotFileName:
                del toc[startPos]
                break
        else:
            print(f"ERROR: File {plotFileName} not found on device {dev}!")
    newData = bytearray(b"BFS0" + b"\0" * 1020)
    for i, startPos, (key, startNonce, nonces, stagger, status, fileName) in enumerate(sorted(toc.items())):
        pos = 4 * i * 32
        newData[pos:pos + 32] = struct.pack("<QQIIQ", key, startNonce, nonces, stagger, (status << 48) | startPos)
    # Write TOC
    with open(dev, "wb") as D:
        D.write(newData)


def adjustPermissions(dev):
    for device in getDevices(dev):
        if hasTOC(device):
            print(f"Adjust permissions for BFS disk {device}.")
            os.chmod(device, 0o664)


def usage():
    print("Usage:")
    print("  l  List plot files and show also read errors.")
    print("  L  List plot files and suppress errors.")
    print("  w  Write plot files from common file system to BFS disk.")
    print("  W  Write and convert to POC2 plot files to BFS disk.")
    print("  r  Read plot files from BFS disk to common file system.")
    print("  d  Delete plot files from BFS disk.")
    print("  c  Convert plot files on BFS disk to POC2.")
    print("  p  Adjust access permissions of BFS devices.")


if __name__ == "__main__":
    print(f"BFS V{VERSION}")
    if len(sys.argv) < 2:
        usage()
        sys.exit(0)
    command = sys.argv[1]
    dev = None if len(sys.argv) < 3 else sys.argv[2]
    name2dev, dev2name = getDeviceList()
    # Either path to device or device name (found in /dev/disk/by-id) is allowed
    if dev in name2dev:
        dev = name2dev[dev]
    if dev is None:
        if command not in {"l", "L", "c", "p"}:
            print("Parameter for disk device is missing!")
            sys.exit(1)
    elif not dev.startswith("/dev/"):
        print("Parameter must be a valid disk device!")
        sys.exit(1)
    t0 = time.time()
    if command == "i":
        if input("Really want to delete all data on disk (y/n)?").lower() == "y":
            initDevice(dev)
    elif command == "l":
        listPlotFiles(dev, dev2name, True)
    elif command == "L":
        listPlotFiles(dev, dev2name, False)
    elif command == "w": # Write to BFS
        writePlotFiles(dev, sys.argv[3:], False)
    elif command == "W": # Write to BFS and convert to PoC2
        writePlotFiles(dev, sys.argv[3:], True)
    elif command == "r":
        readPlotFiles(dev, sys.argv[3:])
    elif command == "d":
        deletePlotFiles(dev, sys.argv[3:])
    elif command == "c":
        convertPlotFiles(dev)
    elif command == "p":
        adjustPermissions(dev)
    print(f"Finished after {int(time.time() - t0)} seconds.")
