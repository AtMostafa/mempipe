"""Data-shape coverage: dtypes, ndim, fixed-shape invariant, deferred init."""

import numpy as np
import pytest

import mempipe


DTYPES = [
    np.float32,
    np.float64,
    np.int8,
    np.int32,
    np.int64,
    np.uint16,
    np.bool_,
    np.complex64,
]


@pytest.mark.parametrize("dtype", DTYPES)
def test_dtype_round_trip(make_pipe, dtype):
    if dtype is np.bool_:
        sent = np.array([[True, False], [False, True]] * 2, dtype=dtype)
    elif np.issubdtype(dtype, np.complexfloating):
        sent = (np.arange(16, dtype=np.float32).reshape(4, 4)
                + 1j * np.arange(16, dtype=np.float32).reshape(4, 4)).astype(dtype)
    elif np.issubdtype(dtype, np.integer):
        sent = np.arange(16, dtype=dtype).reshape(4, 4)
    else:
        sent = np.arange(16, dtype=dtype).reshape(4, 4) / 3

    ex = np.zeros_like(sent)
    mp = make_pipe(ex)
    p_in, p_out = mp.Pipe()

    p_in.send(sent)
    assert p_in.poll(timeout=1.0) is True
    received = p_out.recv()

    assert received.dtype == sent.dtype
    assert received.shape == sent.shape
    assert np.array_equal(received, sent)


@pytest.mark.parametrize(
    "shape",
    [(10,), (4, 4), (2, 3, 4), (1, 100)],
)
def test_shape_round_trip(make_pipe, shape):
    sent = np.random.rand(*shape)
    mp = make_pipe(np.zeros(shape, dtype=np.float64))
    p_in, p_out = mp.Pipe()

    p_in.send(sent)
    assert p_in.poll(timeout=1.0) is True
    received = p_out.recv()

    assert received.shape == sent.shape
    assert np.array_equal(received, sent)


def test_no_ex_array_infers_from_first_send():
    """A MemPipe constructed without ex_array should infer shape/dtype on first send."""
    mp = mempipe.MemPipe()
    try:
        p_in, p_out = mp.Pipe()
        sent = np.arange(12, dtype=np.float64).reshape(3, 4)
        p_in.send(sent)
        assert p_in.poll(timeout=1.0) is True
        received = p_out.recv()
        assert received.shape == sent.shape
        assert received.dtype == sent.dtype
        assert np.array_equal(received, sent)
    finally:
        try:
            mp.close()
        except Exception:
            pass


def test_mismatched_shape_after_init_raises(make_pipe):
    """Once the shape is locked (first send or ex_array), a differently-shaped send must raise."""
    mp = make_pipe(np.zeros((4, 4), dtype=np.float64))
    p_in, _ = mp.Pipe()
    p_in.send(np.ones((4, 4)))
    with pytest.raises(AssertionError):
        p_in.send(np.ones((5, 5)))
