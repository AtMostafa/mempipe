import mempipe
import numpy as np

if __name__ == "__main__":
    mp = mempipe.MemPipe()
    mp.init(np.zeros((10, 10), dtype=np.float32))
    p_in, p_out = mp.Pipe()
    p_in.send(np.ones((10, 10), dtype=np.float32))
    print(p_out.recv())
    p_in.send(np.ones((10, 10), dtype=np.float32))
    print(p_out.recv())
    mp.close()