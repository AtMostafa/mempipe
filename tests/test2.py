import numpy as np
import mempipe
from multiprocessing import Process

def worker(in_conn, out_conn, index):
    """
    Reads a NumPy array from in_conn, adds `index` to it,
    and sends the result to out_conn. Exits on receiving None.
    """
    while True:
        if not in_conn.poll():
            continue
        arr = in_conn.recv()
        if arr is None:
            # Forward the sentinel to the next in the chain
            out_conn.send(None)
            break
        arr = arr + index
        out_conn.send(arr)

if __name__ == "__main__":
    # We create 6 pipes total for a chain of 5 processes:
    # main -> P0 -> P1 -> P2 -> P3 -> P4 -> main
    # So pipe[0] is the channel from main to P0,
    # pipe[1] is the channel from P0 to P1, ...,
    # pipe[5] is the channel from P4 back to main.
    
    # Example data
    arr = np.array([1, 2, 3], dtype=np.int64)
    memp = mempipe.MemPipe(arr)
    NUM_PROCS = 5
    pipes = [memp.Pipe(duplex=False) for _ in range(NUM_PROCS + 1)]
    
    # Spawn the 5 worker processes in a chain
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
    
    print("Received from the final process:", result)
    # result: [11, 12, 13]
    # Send a sentinel (None) to signal each process to stop
    pipes[0][1].send(None)  # This will propagate through the chain
    
    # Read the final sentinel so the last process does not remain blocked
    _ = pipes[NUM_PROCS][0].recv()
    
    # Join all processes
    for p in processes:
        p.join()
