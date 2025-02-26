"The mempipe module provides a simple way to create a pipe between two processes using shared memory."

import multiprocessing as mp
from multiprocessing import shared_memory, Pipe, Lock

import numpy as np


class MemPipe:
    def __init__(self):
        self._init_done = False

    def __del__(self):
        self._shm.close()
        self._shm.unlink()
        self.p_in.close()
        self.p_out.close()

    def init(self, ex_array):
        """
        Initialize the mempipe
        ex_array: example numpy array that will be shared between processes
        ex_array is used to determine the shape and dtype of the shared memory
        """
        assert not self._init_done, "MemPipe already initialized"

        self._shape = ex_array.shape
        self._dtype = ex_array.dtype
        shm_size = ex_array.nbytes
        self._shm = shared_memory.SharedMemory(create=True, size=shm_size)
        self._arr = np.ndarray(self._shape, dtype=self._dtype, buffer=self._shm.buf)
        self._lock = Lock()
        self._init_done = True

    def Pipe(self, *args, **kwargs):
        self.p_in, self.p_out = Pipe(*args, **kwargs)
        return self.p_in, self.p_out

    def send(self, data):
        assert data.shape == self._shape, \
            f"Data shape {data.shape} does not match mempipe shape {self._shape}"
        with self._lock:
            self._arr[:] = data
        self.p_out.send("GO")

    def recv(self):
        with self._lock:
            data = self._arr.copy()
        return data

    def poll(self, *args, **kwargs):
        if not self.p_in.poll(*args, **kwargs):
            return False
        elif self.p_in.recv() == "GO":
            return True
        else:
            raise ValueError("Invalid message received")

    def close(self):
        self._shm.close()
        self._shm.unlink()
        self.p_in.close()
        self.p_out.close()
