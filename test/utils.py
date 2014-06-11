"""Misc testing utils"""

from mock import Mock
from contextlib import contextmanager
from IPython.parallel.error import CompositeError, wrap_exception, \
    unwrap_exception
from time import time
from cloud.serialization.cloudpickle import dumps, loads


class MockView(Mock):
    """A mock IPython cluster view"""

    def __init__(self, *args, **kwargs):
        Mock.__init__(self)
        if kwargs.get("retries"):
            self.tries = kwargs["retries"]
        else:
            self.tries = 1

    def map_sync(self, f, *args):
        res = []
        exceptions = []
        for group in zip(*args):
            # simulate network roundtrip
            group = loads(dumps(group))
            tries = 0
            while tries < self.tries:
                tries += 1
                try:
                    res.append(f(*group))
                except:
                    if tries < self.tries:
                        pass
                    else:
                        # lol
                        exceptions.append(unwrap_exception(wrap_exception()))
                        break
        if exceptions:
            raise CompositeError("Mock Composite error", exceptions)
        else:
            return res


@contextmanager
def mock_view(*args, **kwargs):
    yield MockView(*args, **kwargs)


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
