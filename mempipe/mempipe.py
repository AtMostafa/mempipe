"The mempipe module provides a simple way to create a pipe between two processes using shared memory."

from multiprocessing import shared_memory, Pipe, Lock

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
        self._shm = shared_memory.SharedMemory(create=True, size=shm_size)
        self._arr = np.ndarray(self._shape, dtype=self._dtype, buffer=self._shm.buf)
        self._lock = Lock()
        self._p_in, self._p_out = Pipe(duplex=False)

    def __del__(self):
        self._shm.close()
        self._shm.unlink()
        self._p_in.close()
        self._p_out.close()

    def send(self, data):
        assert data.shape == self._shape, \
            f"Data shape {data.shape} does not match mempipe shape {self._shape}"
        with self._lock:
            self._arr[:] = data
        self._p_out.send("GO")

    def recv(self):
        with self._lock:
            data = self._arr.copy()
        return data

    def poll(self, *args, **kwargs):
        if not self._p_in.poll(*args, **kwargs):
            return False
        elif self._p_in.recv() == "GO":
            return True
        else:
            raise ValueError("Invalid message received")

    def close(self):
        self._shm.close()
        self._shm.unlink()
        self._p_in.close()
        self._p_out.close()

class p_in:
    def poll(self, *args, **kwargs):
        return False
    def recv(self):
        return None
class p_out:
    def send(self, *args, **kwargs):
        pass