"The mempipe module provides a simple way to create a pipe between two processes using shared memory."

from multiprocessing import Pipe, Lock
from multiprocessing.shared_memory import SharedMemory

import numpy as np


class MemPipe:
    def __init__(self, ex_array: np.ndarray | None = None):
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
        self._owns_shm = False
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
            # Sender path: create a new shm region and seed it with ex_array.
            self._shape = ex_array.shape
            self._shm_dtype = ex_array.dtype
            self._shm_size = ex_array.nbytes
            self._shm = SharedMemory(create=True, size=self._shm_size)
            self._shm_name = self._shm.name
            self._owns_shm = True
            self._arr = ex_array
        elif shape is not None and dtype is not None and nbytes is not None and name is not None:
            # Receiver path: attach to an existing shm by name. Must NOT write
            # to the buffer — the sender has already placed valid data there.
            self._shape = tuple(shape)
            self._shm_dtype = dtype
            self._shm_size = nbytes
            self._shm_name = name
            self._shm = SharedMemory(name=self._shm_name)
            self._owns_shm = False
        else:
            raise ValueError("ex_array or (shape, dtype, nbytes, name) must be provided")
        self.shm_created = True

    def Pipe(self, *args, **kwargs):
        "To imitate the multiprocessing.Pipe() interface"
        return self, self

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

    def recv(self):
        if not self._polled:
            return None
        data = self._arr
        self._polled = False
        return data

    def poll(self, *args, **kwargs):
        if not self._p_in.poll(*args, **kwargs):
            return False
        p_data = self._p_in.recv()
        if p_data == "GO":
            self._polled = True
            return True
        if isinstance(p_data, tuple):
            if not self.shm_created:
                self._init_shm(shape=p_data[0], dtype=p_data[1], nbytes=p_data[2], name=p_data[3])
            self._polled = True
            return True
        raise ValueError("Invalid message received")

    def close(self):
        shm = getattr(self, "_shm", None)
        if shm is not None:
            try:
                shm.close()
            except Exception:
                pass
            if getattr(self, "_owns_shm", False):
                try:
                    shm.unlink()
                except Exception:
                    pass
                self._owns_shm = False
            self._shm = None
        for attr in ("_p_in", "_p_out"):
            conn = getattr(self, attr, None)
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def __setstate__(self, state):
        # When a MemPipe is pickled (e.g. during Process.start), the receiving
        # side must not inherit ownership of the shared memory.
        self.__dict__.update(state)
        self._owns_shm = False

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
