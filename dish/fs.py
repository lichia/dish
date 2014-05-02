import os
import shutil


def maybe_mkdir(path):
    if not os.path.exists(path):
        os.mkdir(path)


def liftdir(src, dst):
    """Move everything under `src` to `dst`."""
    for f in os.listdir(src):
        shutil.move(os.path.join(src, f), dst)


def canonicalize(job, targets):
    """Make the list of `targets` canonicalized to the `job`,
    which in this case means formatted with the contents of the job
    and relative paths made absolute w/r/t to the job's workdir. """
    res = []
    for target in targets:
        fmted = target.format(**job)
        res.append(os.path.normpath(os.path.join(job["workdir"], fmted)))
    return res
