"The mempipe module provides a simple way to create a pipe between two processes using shared memory."

from multiprocessing import Pipe, Lock
from multiprocessing.shared_memory import SharedMemory

import numpy as np


class MemPipe:
    def __init__(self, ex_array: np.ndarray = None):
        """
        Initialize the mempipe.
        MemPipes can only transfer numpy arrays of the same shape and dtype. 
        ex_array: example numpy array that will be shared between processes
        ex_array is used to determine the shape and dtype of the shared memory
        If not supplied initially, it will be inferred from the first send.
        """
        self._lock = Lock()
        self._p_in, self._p_out = Pipe(duplex=False)
        self._polled = False
        self.shm_created = False
        if isinstance(ex_array, np.ndarray):
            self._init_shm(ex_array)

    @property
    def _arr(self):
        shm = SharedMemory(name=self._shm_name)
        arr = np.ndarray(self._shape, dtype=self._shm_dtype, buffer=shm.buf)
        with self._lock:
            data = arr.copy()
        return data

    @_arr.setter
    def _arr(self, value):
        shm = SharedMemory(name=self._shm_name)
        arr = np.ndarray(self._shape, dtype=self._shm_dtype, buffer=shm.buf)
        with self._lock:
            arr[:] = value

    def _init_shm(self, ex_array=None, shape=None, dtype=None, nbytes=None, name=None):
        if isinstance(ex_array, np.ndarray):
            self._shape = ex_array.shape
            self._shm_dtype = ex_array.dtype
            self._shm_size = ex_array.nbytes
        elif shape and dtype and nbytes and name:
            self._shape = shape
            self._shm_dtype = dtype
            self._shm_size = nbytes
            self._shm_name = name
            ex_array = np.zeros(shape, dtype=dtype)
        else:
            raise ValueError("ex_array or (shape, dtype, nbytes) must be provided")
        try:
            self._shm = SharedMemory(name=self._shm_name)
        except (FileNotFoundError, TypeError, AttributeError):
            self._shm = SharedMemory(create=True, size=self._shm_size)
            self._shm_name = self._shm.name
            
        self._arr = ex_array
        self.shm_created = True

    def Pipe(self, *args, **kwargs):
        "To imitate the multiprocessing.Pipe() interface"
        if hasattr(self, "_p_in"):
            return self, self
        else:
            raise ValueError("MemPipe not instanciaed")

    def send(self, data):
        if self.shm_created:
            msg = "GO"
        else:
            self._init_shm(data)
            msg = (self._shape, self._shm_dtype, self._shm_size, self._shm_name)

        assert data.shape == self._shape, \
            f"Data shape {data.shape} does not match mempipe shape {self._shape}"
        self._arr = data
        self._p_out.send(msg)
        print('sent' + str(msg))

    def recv(self):
        if not self._polled:
            return None
        data = self._arr
        self._polled = False
        return data

    def poll(self, *args, **kwargs):
        if not self._p_in.poll(*args, **kwargs):
            return False
        else:
            p_data = self._p_in.recv()
            if p_data == "GO":
                self._polled = True
                return True
            elif isinstance(p_data, tuple):
                if not self.shm_created:
                    print('rec'+ str(p_data))
                    self._init_shm(shape=p_data[0], dtype=p_data[1], nbytes=p_data[2], name=p_data[3])
                self._polled = True
                return True
        raise ValueError("Invalid message received")

    def close(self):
        self._shm.close()
        self._shm.unlink()
        self._p_in.close()
        self._p_out.close()
    
    def __del__(self):
        self.close()