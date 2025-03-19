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
        If arrays might change size, use ex_array with the largest size
        Data larger than ex_array will be truncated
        """
        self._lock = Lock()
        self._p_in, self._p_out = Pipe(duplex=False)
        self._polled = False
        if isinstance(ex_array, np.ndarray):
            self._init_shm(ex_array)


    @property
    def _arr(self):
        shm = SharedMemory(name=self._shm_name)
        arr = np.ndarray(self._shape, dtype=self._shm_dtype, buffer=shm.buf)
        idx = tuple(slice(0, s, 1) for s in self.final_shape)
        with self._lock:
            out = arr[idx].copy()
        return out

    @_arr.setter
    def _arr(self, value):
        shm = SharedMemory(name=self._shm_name)
        arr = np.ndarray(self._shape, dtype=self._shm_dtype, buffer=shm.buf)
        self.final_shape = [min(value.shape[i], self._shape[i]) for i in range(len(self._shape))]
        idx = tuple(slice(0, s, 1) for s in self.final_shape)
        with self._lock:
            arr[idx] = value[idx]

    def _init_shm(self, ex_array):
        self.final_shape = ex_array.shape
        self._shape = ex_array.shape
        self._shm_dtype = ex_array.dtype
        shm_size = ex_array.nbytes
        self._shm = SharedMemory(create=True, size=shm_size)
        self._shm_name = self._shm.name
        self._arr = ex_array


    def Pipe(self, *args, **kwargs):
        "To imitate the multiprocessing.Pipe() interface"
        if hasattr(self, "final_shape"):
            return self, self
        else:
            raise ValueError("MemPipe not instanciated")

    def send(self, data):
        self._arr = data
        self._p_out.send(self.final_shape)

    def recv(self):
        if not self._polled:
            return None
        data = self._arr
        self._polled = False
        return data

    def poll(self, *args, **kwargs):
        if not self._p_in.poll(*args, **kwargs):
            return False
        self.final_shape = self._p_in.recv()
        self._polled = True
        return True

    def close(self):
        self._shm.close()
        self._shm.unlink()
        self._p_in.close()
        self._p_out.close()
    
    def __del__(self):
        self.close()