"""End-to-end multi-process round-trip tests.

These mirror the existing tests/test2.py demo but with assertions, smaller
arrays, and explicit timeouts so they fit a CI suite.

Workers are defined at module top level so they're picklable under the
'spawn' start method (required on Windows).

NOTE: We use the default `multiprocessing.Process` (not an explicit
get_context("spawn")) so that the lock/pipe primitives inside MemPipe —
which are also created from the default context — match the Process's
start method. Mixing a default-context Lock with an explicit spawn-context
Process hangs on Linux (the cross-context semaphore is unusable).
"""

import multiprocessing
import time

import numpy as np

import mempipe


def _add_one_worker(in_conn, out_conn):
    """Read one array, add 1, send the result."""
    deadline = time.perf_counter() + 30
    while time.perf_counter() < deadline:
        if in_conn.poll(timeout=0.1):
            arr = in_conn.recv()
            out_conn.send(arr + 1)
            return


def _multiply_worker(in_conn, out_conn, factor):
    deadline = time.perf_counter() + 30
    while time.perf_counter() < deadline:
        if in_conn.poll(timeout=0.1):
            arr = in_conn.recv()
            out_conn.send(arr * factor)
            return


def _deferred_init_worker(in_conn, out_conn):
    """Worker that uses MemPipes with no ex_array; shape inferred from first send."""
    deadline = time.perf_counter() + 30
    while time.perf_counter() < deadline:
        if in_conn.poll(timeout=0.1):
            arr = in_conn.recv()
            out_conn.send(arr + 100)
            return


def _ownership_probe(mp, result_q):
    """Spawned worker: report whether the unpickled MemPipe inherited shm ownership."""
    result_q.put(mp._owns_shm)


def test_pickle_roundtrip_drops_shm_ownership():
    """When a MemPipe is pickled during Process.start, the child must NOT
    inherit _owns_shm. Otherwise on Linux the child's __del__ would unlink
    the parent's shared memory out from under it. (Windows masks the bug
    because SharedMemory.unlink() is a no-op there.)

    Uses an explicit spawn context here: this test only exercises pickle
    state — no Lock/Pipe ops happen on the child — so the cross-context
    Lock hang does not apply.
    """
    ctx = multiprocessing.get_context("spawn")
    mp = mempipe.MemPipe(np.zeros((4, 4), dtype=np.float64))
    try:
        assert mp._owns_shm is True
        result_q = ctx.Queue()
        proc = ctx.Process(target=_ownership_probe, args=(mp, result_q))
        proc.start()
        try:
            child_owns_shm = result_q.get(timeout=10)
        finally:
            proc.join(timeout=5)
            if proc.is_alive():
                proc.terminate()
                proc.join(timeout=2)
        assert child_owns_shm is False
        # Parent must still own the shm after the child has come and gone.
        assert mp._owns_shm is True
    finally:
        mp.close()


def test_deferred_init_two_process_echo():
    """Two-process round trip using MemPipe() with NO ex_array.

    This exercises the on-the-fly shm-creation path: the parent's first send()
    creates the shm and the child attaches by name on its first poll().
    """
    pipe_to_worker = mempipe.MemPipe().Pipe()
    pipe_from_worker = mempipe.MemPipe().Pipe()

    proc = multiprocessing.Process(
        target=_deferred_init_worker,
        args=(pipe_to_worker[0], pipe_from_worker[1]),
    )
    proc.start()
    try:
        sent = np.arange(12, dtype=np.int64).reshape(3, 4)
        pipe_to_worker[1].send(sent)

        deadline = time.perf_counter() + 10
        while time.perf_counter() < deadline:
            if pipe_from_worker[0].poll(timeout=0.1):
                received = pipe_from_worker[0].recv()
                break
        else:
            raise AssertionError("worker did not respond within 10s")

        assert received.shape == sent.shape
        assert received.dtype == sent.dtype
        assert np.array_equal(received, sent + 100)
    finally:
        proc.join(timeout=2.0)
        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=2.0)


def test_two_process_echo():
    """Parent → worker → parent round trip via two MemPipes."""
    ex = np.zeros((50, 50), dtype=np.float64)

    pipe_to_worker = mempipe.MemPipe(ex).Pipe()
    pipe_from_worker = mempipe.MemPipe(ex).Pipe()

    proc = multiprocessing.Process(
        target=_multiply_worker,
        args=(pipe_to_worker[0], pipe_from_worker[1], 2.0),
    )
    proc.start()
    try:
        sent = np.random.rand(50, 50)
        pipe_to_worker[1].send(sent)

        deadline = time.perf_counter() + 10
        while time.perf_counter() < deadline:
            if pipe_from_worker[0].poll(timeout=0.1):
                received = pipe_from_worker[0].recv()
                break
        else:
            raise AssertionError("worker did not respond within 10s")

        assert received.shape == sent.shape
        assert np.allclose(received, sent * 2.0)
    finally:
        proc.join(timeout=2.0)
        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=2.0)


def test_five_process_pipeline():
    """Linear chain of 5 workers, each adding 1; result should equal input + 5."""
    ex = np.zeros((50, 50), dtype=np.float64)
    NUM_PROCS = 5

    pipes = [mempipe.MemPipe(ex).Pipe() for _ in range(NUM_PROCS + 1)]

    processes = []
    for i in range(NUM_PROCS):
        p = multiprocessing.Process(
            target=_add_one_worker,
            args=(pipes[i][0], pipes[i + 1][1]),
        )
        p.start()
        processes.append(p)

    try:
        sent = np.random.rand(50, 50)
        pipes[0][1].send(sent)

        deadline = time.perf_counter() + 15
        while time.perf_counter() < deadline:
            if pipes[NUM_PROCS][0].poll(timeout=0.1):
                result = pipes[NUM_PROCS][0].recv()
                break
        else:
            raise AssertionError("pipeline did not produce result within 15s")

        assert result.shape == sent.shape
        assert np.allclose(result, sent + NUM_PROCS)
    finally:
        for p in processes:
            p.join(timeout=2.0)
            if p.is_alive():
                p.terminate()
                p.join(timeout=2.0)
