"The mempipe module provides a simple way to create a pipe between two processes using shared memory."

from multiprocessing import Pipe, Lock
from multiprocessing.shared_memory import SharedMemory

import numpy as np


class MemPipe:
    def __init__(self, ex_array: np.ndarray):
        """
        Initialize the mempipe
        ex_array: example numpy array that will be shared between processes
        ex_array is used to determine the shape and dtype of the shared memory
        """
        self._shape = ex_array.shape
        self._dtype = ex_array.dtype
        shm_size = ex_array.nbytes
        self._shm = SharedMemory(create=True, size=shm_size)
        self._arr = np.ndarray(self._shape, dtype=self._dtype, buffer=self._shm.buf)
        self._lock = Lock()
        self._p_in, self._p_out = Pipe(duplex=False)
        self._polled = False

    def Pipe(self, *args, **kwargs):
        "To imitate the multiprocessing.Pipe() interface"
        if hasattr(self, "_p_in"):
            return self, self
        else:
            raise ValueError("MemPipe not initialised")

    def send(self, data):
        assert data.shape == self._shape, \
            f"Data shape {data.shape} does not match mempipe shape {self._shape}"
        with self._lock:
            self._arr[:] = data
        self._p_out.send("GO")

    def recv(self):
        if not self._polled:
            return None
        with self._lock:
            data = self._arr.copy()
        self._polled = False
        return data

    def poll(self, *args, **kwargs):
        if not self._p_in.poll(*args, **kwargs):
            return False
        elif self._p_in.recv() == "GO":
            self._polled = True
            return True
        else:
            raise ValueError("Invalid message received")

    def close(self):
        self._shm.close()
        self._shm.unlink()
        self._p_in.close()
        self._p_out.close()
    
    def __del__(self):
        self.close()