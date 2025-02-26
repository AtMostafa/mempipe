import mempipe
import numpy as np

if __name__ == "__main__":
    mp = mempipe.MemPipe(np.zeros((10, 10), dtype=np.float64))
    p_in, p_out = mp.Pipe()
    p_in.send(np.random.rand(10, 10))
    p_in.poll()
    print(p_out.recv())
    mp.close()