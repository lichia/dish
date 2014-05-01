import os
import shutil
import tempfile

from contextlib import contextmanager


def maybe_mkdir(path):
    if not os.path.exists(path):
        os.mkdir(path)


def liftdir(src, dst):
    """Move everything under `src` to `dst`."""
    for f in os.listdir(src):
        shutil.move(os.path.join(src, f), dst)
