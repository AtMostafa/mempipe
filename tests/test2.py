import numpy as np
import mempipe
from multiprocessing import Process, Pipe

def worker(in_conn, out_conn, index):
    """
    Reads a NumPy array from in_conn, adds `index` to it,
    and sends the result to out_conn. Exits on receiving None.
    """
    while True:
        if not in_conn.poll():
            continue
        arr = in_conn.recv()
        arr = arr + 1
        print(f'Worker {index} sent:', arr)
        out_conn.send(arr)

if __name__ == "__main__":
    # We create 6 pipes total for a chain of 5 processes:
    # main -> P0 -> P1 -> P2 -> P3 -> P4 -> main
    # So pipe[0] is the channel from main to P0,
    # pipe[1] is the channel from P0 to P1, ...,
    # pipe[5] is the channel from P4 back to main.
    
    # Example data
    arr = np.random.rand(1200,200)
    NUM_PROCS = 5
    pipes = [mempipe.MemPipe(arr).Pipe() for _ in range(NUM_PROCS + 1)]
    # pipes = [Pipe(duplex=False) for _ in range(NUM_PROCS + 1)]
    
    # Spawn the 5 worker processes in a chain
    processes = []
    for i in range(NUM_PROCS):
        # in_conn for worker i is pipes[i][0],
        # out_conn for worker i is pipes[i+1][1]
        p = Process(target=worker, args=(pipes[i][0], pipes[i+1][1], i))
        p.start()
        processes.append(p)

    # Send the array to the first process via pipes[0][1]
    arr1 = np.random.rand(1200,200)
    pipes[0][1].send(arr1)
    
    # Receive the processed array from the last process via pipes[5][0]
    while True:
        if pipes[NUM_PROCS][0].poll():
            result = pipes[NUM_PROCS][0].recv()
            break
    
    print("Received from the final process:", result)
    if np.allclose(result, arr1 + NUM_PROCS):
        print("Results match!")
    else:
        print("Results do not match!")
    
    # Join all processes
    for p in pipes:
        try:
            p[0].close()
        except:
            pass

    for p in processes:
        p.join(.1)
        p.terminate()
