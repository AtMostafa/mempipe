"""poll() semantics and ordering invariants around poll/recv."""

import time

import numpy as np


def test_poll_zero_timeout_on_empty(make_pipe):
    mp = make_pipe(np.zeros((4, 4), dtype=np.float64))
    p_in, _ = mp.Pipe()
    assert p_in.poll(timeout=0) is False


def test_poll_bounded_wait_on_empty(make_pipe):
    mp = make_pipe(np.zeros((4, 4), dtype=np.float64))
    p_in, _ = mp.Pipe()

    t0 = time.perf_counter()
    result = p_in.poll(timeout=0.05)
    elapsed = time.perf_counter() - t0

    assert result is False
    # Generous bounds — Windows timer granularity is ~15ms.
    assert 0.03 <= elapsed <= 0.5


def test_poll_returns_true_quickly_after_send(make_pipe):
    mp = make_pipe(np.zeros((4, 4), dtype=np.float64))
    p_in, p_out = mp.Pipe()

    p_in.send(np.ones((4, 4)))
    t0 = time.perf_counter()
    result = p_in.poll(timeout=1.0)
    elapsed = time.perf_counter() - t0

    assert result is True
    assert elapsed < 0.5
    # Drain so the fixture's close() doesn't block.
    p_out.recv()


def test_second_recv_after_one_poll_returns_none(make_pipe):
    """One poll() arms exactly one recv(). The next recv() before another poll()
    returns None, per the _polled flag at mempipe.py:66-69."""
    mp = make_pipe(np.zeros((4, 4), dtype=np.float64))
    p_in, p_out = mp.Pipe()

    p_in.send(np.full((4, 4), 7.0))
    assert p_in.poll(timeout=1.0) is True
    first = p_out.recv()
    assert np.array_equal(first, np.full((4, 4), 7.0))

    second = p_out.recv()
    assert second is None


def test_overlapping_sends_share_memory_buffer(make_pipe):
    """Two sends before any poll: the pipe queues two shape messages, but the
    shared-memory buffer holds only the latest value. Both recvs therefore
    return B (the last write). This pins down the actual library behavior so
    a future change is visible.
    """
    mp = make_pipe(np.zeros((4, 4), dtype=np.float64))
    p_in, p_out = mp.Pipe()

    A = np.full((4, 4), 1.0)
    B = np.full((4, 4), 2.0)
    p_in.send(A)
    p_in.send(B)

    assert p_in.poll(timeout=1.0) is True
    first = p_out.recv()
    assert p_in.poll(timeout=1.0) is True
    second = p_out.recv()

    # Buffer was overwritten by B before either recv ran.
    assert np.array_equal(first, B)
    assert np.array_equal(second, B)
