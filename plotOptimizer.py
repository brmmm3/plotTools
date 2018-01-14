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


def readerThread(pathName, nonces, stagger, buf, sem, lock):
    groupCnt = nonces // stagger
    groupSize = stagger * NONCE_SIZE
    groupScoopSize = stagger * SCOOP_SIZE
    try:
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
        print(BRIGHTGREEN + "BURST plot optimizer (version 1.0)")
        print(BRIGHTBLUE + "Usage: %s INPATH [OUTDIR]..." % sys.argv[0])
        print(BRIGHTGREEN + "If OUTDIR is missing then the optimized plot is written to the same directory " 
              "as the source plot." + RESET_ALL)
        sys.exit(1)
    inPathName = sys.argv[1]
    outDirName = sys.argv[2] if len(sys.argv) > 2 else os.path.dirname(inPathName)
    try:
        key, startnonce, nonces, stagger = [ int(x) for x in os.path.basename(inPathName).split("_") ]
    except Exception as exc:
        print(BRIGHTRED + f"Error: Invalid source filename: {exc}" + RESET_ALL)
        sys.exit(1)
    inSize = os.path.getsize(inPathName)
    if nonces * NONCE_SIZE != inSize:
        print(BRIGHTRED + f"Error: Source file has invalid size! Expected {nonces * NONCE_SIZE} but file has {inSize}!"
              + RESET_ALL)
        sys.exit(1)
    groupCnt = nonces // stagger
    if groupCnt == 1:
        print(BRIGHTGREEN + "Source file is already optimized!" + RESET_ALL)
        sys.exit(1)
    if nonces != groupCnt * stagger:
        print(BRIGHTRED + "Error: Source file has invalid nonces or stagger!" + RESET_ALL)
        sys.exit(1)
    if diskFree(outDirName) < inSize:
        print(BRIGHTRED + "Error: Not enough free space on disk for optimized plot file!" + RESET_ALL)
        sys.exit(1)
    groupScoopSize = stagger * SCOOP_SIZE
    outPathName = os.path.join(outDirName, f"{key}_{startnonce}_{nonces}_{nonces}")
    print(BRIGHTYELLOW + "Source file has:")
    print(f"  Nonces:              {nonces}")
    print(f"  Groups:              {groupCnt}")
    print(f"  Stagger size:        {stagger}")
    print(f"  Scoop size in group: {groupScoopSize}")
    print(f"Destination file: {outPathName}" + RESET_ALL)
    if os.path.exists(outPathName):
        print(BRIGHTRED + f"Warning: Destination file {outPathName} already exists! Removing it!" + RESET_ALL)
        os.remove(outPathName)
    print(BRIGHTGREEN + f"Writing optimized plot to {outPathName}..." + RESET_ALL)
    buf = deque()
    sem = Semaphore(1000)
    lock = _thread.allocate_lock()
    thrReader = Thread(target = readerThread, args = ( inPathName, nonces, stagger, buf, sem, lock ), daemon = True)
    thrReader.start()
    cnt = written = lastWritten = 0
    t0 = t1 = time.time()
    with open(outPathName, "wb") as O:
        while thrReader.is_alive() or buf:
            try:
                data = buf.popleft()
                O.write(data)
                sem.release()
                cnt += 1
                written += len(data)
                if cnt >= 1000:
                    t2 = time.time()
                    print("%.1f%% written. %d MB/s. " % (100 * written / inSize, (written - lastWritten) / (t2 - t1)),
                          end = "\r")
                    cnt = 0
                    lastWritten = written
                    t1 = t2
            except:
                lock.acquire()
    print(BRIGHTGREEN + f"Finished after {int(time.time() - t0)} seconds.")
