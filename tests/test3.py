import numpy as np
import mempipe
from multiprocessing import Process, Pipe
from time import perf_counter

def worker(in_conn, out_conn, index):
    """
    Reads a NumPy array from in_conn, adds `index` to it,
    and sends the result to out_conn. Exits on receiving None.
    """
    while True:
        if not in_conn.poll():
            continue
        arr = in_conn.recv()
        arr = arr + index
        out_conn.send(arr)

def run_pipeline(use_mempipe=False):
    # We create 6 pipes total for a chain of 5 processes:
    # main -> P0 -> P1 -> P2 -> P3 -> P4 -> main
    # So pipe[0] is the channel from main to P0,
    # pipe[1] is the channel from P0 to P1, ...,
    # pipe[5] is the channel from P4 back to main.
    
    # Example data
    arr = np.random.rand(12000,4000)
    NUM_PROCS = 5
    if use_mempipe:
        pipes = [mempipe.MemPipe(arr).Pipe() for _ in range(NUM_PROCS + 1)]
    else:
        pipes = [Pipe(duplex=False) for _ in range(NUM_PROCS + 1)]

    # Spawn the 5 worker processes in a chain
    t0 = perf_counter()
    processes = []
    for i in range(NUM_PROCS):
        # in_conn for worker i is pipes[i][0],
        # out_conn for worker i is pipes[i+1][1]
        p = Process(target=worker, args=(pipes[i][0], pipes[i+1][1], i))
        p.start()
        processes.append(p)

    # Send the array to the first process via pipes[0][1]
    pipes[0][1].send(arr)
    
    # Receive the processed array from the last process via pipes[5][0]
    while True:
        if pipes[NUM_PROCS][0].poll():
            result = pipes[NUM_PROCS][0].recv()
            break
    out = perf_counter() - t0
    print("Received from the final process:", result.shape)
    print(f'{np.all(result == arr)}')
    
    # Join all processes
    for p in pipes:
        try:
            p[0].close()
        except:
            pass
    for p in processes:
        p.join(.1)
        p.terminate()
    
    return out

if __name__ == "__main__":
    from matplotlib import pyplot as plt
    mempipe_time = []
    for _ in range(20):
        a = run_pipeline(use_mempipe=True)
        mempipe_time.append(a)
    plt.plot(mempipe_time, label='MemPipe')
    
    pipe_time = []
    for _ in range(20):
        a = run_pipeline(use_mempipe=False)
        pipe_time.append(a)
    plt.plot(pipe_time, label='Pipe')
    plt.legend()
    plt.show()    
