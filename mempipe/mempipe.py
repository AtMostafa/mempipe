"The mempipe module provides a simple way to create a pipe between two processes using shared memory."

from multiprocessing import Pipe, Lock
from multiprocessing.shared_memory import SharedMemory

import numpy as np


class MemPipe:
    def __init__(self, ex_array: np.ndarray):
        """
        Initialize the mempipe.
        MemPipes can only transfer numpy arrays of the same shape and dtype. 
        ex_array: example numpy array that will be shared between processes
        ex_array is used to determine the shape and dtype of the shared memory
        If not supplied initially, it will be inferred from the first send.
        """
        if isinstance(ex_array, np.ndarray):
            self._init_shm(ex_array)
        self._lock = Lock()
        self._p_in, self._p_out = Pipe(duplex=False)
        self._polled = False

    @property
    def _arr(self):
        shm = SharedMemory(name=self._shm_name)
        arr = np.ndarray(self._shape, dtype=self._shm_dtype, buffer=shm.buf)
        return arr.copy()

    @_arr.setter
    def _arr(self, value):
        shm = SharedMemory(name=self._shm_name)
        arr = np.ndarray(self._shape, dtype=self._shm_dtype, buffer=shm.buf)
        arr[:] = value

    def _init_shm(self, ex_array):
        self._shape = ex_array.shape
        self._shm_dtype = ex_array.dtype
        shm_size = ex_array.nbytes
        self._shm = SharedMemory(create=True, size=shm_size)
        self._shm_name = self._shm.name
        self._arr = ex_array


    def Pipe(self, *args, **kwargs):
        "To imitate the multiprocessing.Pipe() interface"
        if hasattr(self, "_p_in"):
            return self, self
        else:
            raise ValueError("MemPipe not instanciaed")

    def send(self, data):
        assert data.shape == self._shape, \
            f"Data shape {data.shape} does not match mempipe shape {self._shape}"
        with self._lock:
            self._arr = data
        self._p_out.send("GO")

    def recv(self):
        if not self._polled:
            return None
        with self._lock:
            data = self._arr
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