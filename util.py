
import os
import _thread

SCOOP_SIZE = 64
NUM_SCOOPS = 4096
NONCE_SIZE = NUM_SCOOPS * SCOOP_SIZE


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
