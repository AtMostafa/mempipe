"""Tests for the passthrough channel: non-ndarray values travel through the
underlying multiprocessing pipe (wrapped in a `('__pt__', value)` sentinel)
instead of shared memory.

In-process cases use the `make_pipe` fixture (tests/conftest.py). Multiprocess
workers are defined at module top level so they're picklable under the 'spawn'
start method, mirroring tests/test_multiprocess.py.
"""

import multiprocessing
import time

import numpy as np
import pytest

import mempipe
from mempipe.mempipe import _PASSTHROUGH


def _poll_recv(pipe, timeout=1.0):
    """Poll then recv, asserting poll() armed a value."""
    assert pipe.poll(timeout=timeout) is True
    return pipe.recv()


def test_passthrough_string(make_pipe):
    mp = make_pipe(np.zeros((4, 4), dtype=np.float64))
    p_in, p_out = mp.Pipe()

    p_in.send("hello world")
    assert _poll_recv(p_out) == "hello world"


@pytest.mark.parametrize(
    "value",
    [
        ["a", "b", "c"],
        {"k": 1, "nested": {"x": [1, 2]}},
        (1, 2, 3),
        42,
        None,
        3.14,
    ],
)
def test_passthrough_various_types(make_pipe, value):
    mp = make_pipe(np.zeros((4, 4), dtype=np.float64))
    p_in, p_out = mp.Pipe()

    p_in.send(value)
    assert _poll_recv(p_out) == value


def test_passthrough_works_without_ex_array():
    """A MemPipe that never created shm can still carry passthrough values."""
    mp = mempipe.MemPipe()
    try:
        p_in, p_out = mp.Pipe()
        p_in.send("control-message")
        assert _poll_recv(p_out) == "control-message"
        # Nothing should have been allocated for a pure-passthrough exchange.
        assert mp.shm_created is False
    finally:
        mp.close()


def test_passthrough_tuple_value_is_not_confused_with_init(make_pipe):
    """Value tuples — including ones shaped like the sentinel itself — must
    round-trip intact thanks to the wrapping in send()."""
    mp = make_pipe(np.zeros((4, 4), dtype=np.float64))
    p_in, p_out = mp.Pipe()

    plain = (10, 20)
    p_in.send(plain)
    assert _poll_recv(p_out) == plain

    sneaky = (_PASSTHROUGH, "payload")
    p_in.send(sneaky)
    assert _poll_recv(p_out) == sneaky


def test_recv_without_poll_returns_none_for_passthrough(make_pipe):
    mp = make_pipe(np.zeros((4, 4), dtype=np.float64))
    p_in, p_out = mp.Pipe()

    p_in.send("queued-but-not-polled")
    assert p_out.recv() is None


def test_interleaved_array_then_passthrough(make_pipe):
    """Array round-trip followed by a passthrough on the SAME pipe.

    Exercises the array recv branch and the shm-init poll branch, then the
    passthrough poll/recv branches.
    """
    mp = make_pipe(np.zeros((3, 3), dtype=np.float64))
    p_in, p_out = mp.Pipe()

    arr = np.arange(9, dtype=np.float64).reshape(3, 3)
    p_in.send(arr)
    received = _poll_recv(p_out)
    assert np.array_equal(received, arr)

    p_in.send({"status": "done"})
    assert _poll_recv(p_out) == {"status": "done"}


def test_interleaved_passthrough_then_array(make_pipe):
    """Passthrough first, then an array round-trip on the same pipe."""
    mp = make_pipe(np.zeros((3, 3), dtype=np.float64))
    p_in, p_out = mp.Pipe()

    p_in.send("prelude")
    assert _poll_recv(p_out) == "prelude"

    arr = np.full((3, 3), 7.0)
    p_in.send(arr)
    received = _poll_recv(p_out)
    assert np.array_equal(received, arr)


def test_deferred_init_array_then_passthrough():
    """No ex_array: the first array send creates shm and emits the 4-tuple
    init message, exercising poll()'s shm-init branch (and its flag reset)
    distinct from the 'GO' path. A passthrough then follows on the same pipe.
    """
    mp = mempipe.MemPipe()
    try:
        p_in, p_out = mp.Pipe()

        arr = np.arange(6, dtype=np.float64).reshape(2, 3)
        p_in.send(arr)                       # shm_created False -> 4-tuple init msg
        received = _poll_recv(p_out)         # poll consumes the init tuple
        assert np.array_equal(received, arr)

        p_in.send("after-array")
        assert _poll_recv(p_out) == "after-array"
    finally:
        mp.close()


def test_defensive_flag_reset_after_unconsumed_passthrough_poll(make_pipe):
    """Send a passthrough then an array; poll twice WITHOUT a recv in between.

    The second poll consumes the array-init tuple and must clear the stale
    passthrough flag, so recv() returns the array — not the earlier string.
    """
    mp = make_pipe(np.zeros((2, 2), dtype=np.float64))
    p_in, p_out = mp.Pipe()

    p_in.send("stale")
    arr = np.full((2, 2), 5.0)
    p_in.send(arr)

    assert p_out.poll(timeout=1.0) is True   # arms passthrough "stale"
    assert p_out.poll(timeout=1.0) is True   # arms the array, clears the flag

    received = p_out.recv()
    assert isinstance(received, np.ndarray)
    assert np.array_equal(received, arr)


def test_setstate_backfills_passthrough_attrs():
    """Unpickling state that predates the passthrough attrs must backfill them
    to safe defaults and drop shm ownership.

    Pickle calls __setstate__ on a freshly __new__'d instance whose __dict__ is
    empty, so we reproduce that (not a re-set on an existing object) to actually
    hit the backfill guards. A minimal state with no shm/pipe refs keeps the
    instance's __del__/close() harmless.
    """
    fresh = mempipe.MemPipe.__new__(mempipe.MemPipe)
    fresh.__setstate__({"_owns_shm": True})

    assert fresh._is_passthrough is False
    assert fresh._passthrough_val is None
    assert fresh._owns_shm is False


# --- Multiprocess end-to-end ------------------------------------------------

def _passthrough_echo_worker(in_conn, out_conn):
    """Read one control string, send back an 'ack:'-prefixed reply."""
    deadline = time.perf_counter() + 30
    while time.perf_counter() < deadline:
        if in_conn.poll(timeout=0.1):
            msg = in_conn.recv()
            out_conn.send("ack:" + msg)
            return


def test_passthrough_across_processes():
    """A passthrough string survives a real process boundary and the pipeline
    output matches the expected transformation."""
    pipe_to_worker = mempipe.MemPipe().Pipe()
    pipe_from_worker = mempipe.MemPipe().Pipe()

    proc = multiprocessing.Process(
        target=_passthrough_echo_worker,
        args=(pipe_to_worker[0], pipe_from_worker[1]),
    )
    proc.start()
    try:
        pipe_to_worker[1].send("ping")

        deadline = time.perf_counter() + 10
        received = None
        while time.perf_counter() < deadline:
            if pipe_from_worker[0].poll(timeout=0.1):
                received = pipe_from_worker[0].recv()
                break
        else:
            raise AssertionError("worker did not respond within 10s")

        assert received == "ack:ping"
    finally:
        proc.join(timeout=2.0)
        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=2.0)
        for mp in (pipe_to_worker[0], pipe_from_worker[0]):
            try:
                mp.close()
            except Exception:
                pass
