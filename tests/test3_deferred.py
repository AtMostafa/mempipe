"""Real-world benchmark of the deferred-init MemPipe API.

Mirrors tests/test3.py — a 5-process linear pipeline passing a (12000, 4000)
float64 array (~384 MB) — but constructs each `MemPipe()` WITHOUT an
ex_array. The shape and dtype are inferred from the first send() on each
pipe, exactly as described in the README:

    "If not supplied initially, it will be inferred from the first send."

Asserts that the round-trip data is correct (result == arr + sum(0..NUM_PROCS-1))
and prints timing for MemPipe vs. plain multiprocessing.Pipe.

Run with:
    uv run python tests/test3_deferred.py
"""

import numpy as np
from mempipe import MemPipe
from multiprocessing import Pipe, get_context
from time import perf_counter


def worker(in_conn, out_conn, index):
    """Read one array, add `index`, forward, exit."""
    while True:
        if not in_conn.poll(timeout=0.05):
            continue
        arr = in_conn.recv()
        out_conn.send(arr + index)
        return


def run_pipeline(use_mempipe, arr, num_procs):
    ctx = get_context("spawn")
    if use_mempipe:
        pipes = [MemPipe().Pipe() for _ in range(num_procs + 1)]
    else:
        pipes = [Pipe(duplex=False) for _ in range(num_procs + 1)]

    t0 = perf_counter()
    processes = []
    for i in range(num_procs):
        p = ctx.Process(target=worker, args=(pipes[i][0], pipes[i + 1][1], i))
        p.start()
        processes.append(p)

    pipes[0][1].send(arr)

    while True:
        if pipes[num_procs][0].poll(timeout=60):
            result = pipes[num_procs][0].recv()
            break
        raise TimeoutError("pipeline produced no result within 60s")
    elapsed = perf_counter() - t0

    for p in processes:
        p.join(timeout=1.0)
        if p.is_alive():
            p.terminate()

    return result, elapsed


def main():
    NUM_PROCS = 5
    EXPECTED_DELTA = sum(range(NUM_PROCS))  # 0+1+2+3+4 = 10
    SHAPE = (12000, 4000)
    nbytes_mb = int(np.prod(SHAPE)) * 8 / 1e6
    print(f"Pipeline: {NUM_PROCS} workers, expected delta = +{EXPECTED_DELTA}")
    print(f"Payload: shape={SHAPE}, dtype=float64 (~{nbytes_mb:.0f} MB per shm)")

    arr = np.random.rand(*SHAPE)

    print("\n[MemPipe — deferred init]")
    result_mp, t_mp = run_pipeline(use_mempipe=True, arr=arr, num_procs=NUM_PROCS)
    print(f"  elapsed: {t_mp:.3f}s")

    print("\n[multiprocessing.Pipe]")
    result_p, t_p = run_pipeline(use_mempipe=False, arr=arr, num_procs=NUM_PROCS)
    print(f"  elapsed: {t_p:.3f}s")

    print("\nCorrectness assertions:")
    assert result_mp.shape == arr.shape, f"MemPipe shape: {result_mp.shape}"
    assert result_mp.dtype == arr.dtype, f"MemPipe dtype: {result_mp.dtype}"
    assert np.allclose(result_mp, arr + EXPECTED_DELTA), "MemPipe data mismatch"
    print(f"  MemPipe: shape={result_mp.shape}, dtype={result_mp.dtype}, data OK")

    assert result_p.shape == arr.shape
    assert np.allclose(result_p, arr + EXPECTED_DELTA), "Pipe data mismatch"
    print(f"  Pipe:    shape={result_p.shape}, dtype={result_p.dtype}, data OK")

    print(f"\nSpeedup (MemPipe vs Pipe): {t_p / t_mp:.2f}x")


if __name__ == "__main__":
    main()
