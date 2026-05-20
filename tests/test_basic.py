"""Single-process happy-path tests for MemPipe."""

import numpy as np


def test_round_trip_preserves_values(make_pipe):
    mp = make_pipe(np.zeros((10, 10), dtype=np.float64))
    p_in, p_out = mp.Pipe()

    sent = np.random.rand(10, 10)
    p_in.send(sent)
    assert p_in.poll(timeout=1.0) is True

    received = p_out.recv()
    assert np.array_equal(received, sent)


def test_recv_before_poll_returns_none(make_pipe):
    mp = make_pipe(np.zeros((4, 4), dtype=np.float64))
    p_in, p_out = mp.Pipe()

    p_in.send(np.ones((4, 4)))
    assert p_out.recv() is None


def test_repeated_send_recv_cycles(make_pipe):
    mp = make_pipe(np.zeros((5, 5), dtype=np.float64))
    p_in, p_out = mp.Pipe()

    for i in range(3):
        payload = np.full((5, 5), float(i))
        p_in.send(payload)
        assert p_in.poll(timeout=1.0) is True
        received = p_out.recv()
        assert np.array_equal(received, payload)


def test_pipe_returns_self_self_tuple(make_pipe):
    mp = make_pipe(np.zeros((3, 3), dtype=np.float64))
    p_in, p_out = mp.Pipe()
    assert p_in is mp
    assert p_out is mp
