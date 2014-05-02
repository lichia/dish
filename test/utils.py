"""Misc testing utils"""

from mock import MagicMock
from contextlib import contextmanager
from IPython.parallel.error import CompositeError
from time import time

class MockView(MagicMock):
    """A mock IPython cluster view"""

    def map_sync(self, f, *args):
        res = []
        exceptions = []
        for group in zip(*args):
            try:
                res.append(f(*group))
            except Exception as e:
                exceptions.append(e)
        if exceptions:
            raise CompositeError("Mock Composite error", exceptions)
        else:
            return res


@contextmanager
def mock_view(*args, **kwargs):
    yield MockView()


def assert_eventually_equal(*args, **kwargs):
    if "to_wait" in kwargs.keys():
        to_wait = kwargs["to_wait"]
    else:
        to_wait = 1
    start = time()
    while True:
        try:
            curr = args[0]
            for val in args:
                assert curr == val
                curr = val
        except:
            if time() - start > to_wait:
                raise AssertionError("values did not eventually become equal")
        return
