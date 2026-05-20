import pytest

import mempipe


@pytest.fixture
def make_pipe():
    """Construct a MemPipe and guarantee close() is called afterwards.

    MemPipe.__del__ unlinks shared memory; without explicit close() on failure
    paths we can leak resources between tests.
    """
    created = []

    def _make(ex_array):
        mp = mempipe.MemPipe(ex_array)
        created.append(mp)
        return mp

    yield _make

    for mp in created:
        try:
            mp.close()
        except Exception:
            pass
