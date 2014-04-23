import os

def maybe_mkdir(path):
    if not os.path.exists(path):
        os.mkdir(path)
