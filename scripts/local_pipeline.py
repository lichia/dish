import tempfile
import os
from dish.pipeline import Pipeline


workdir = tempfile.mkdtemp()
os.chdir(workdir)
jobs = [{"description": "test1"}, {"description": "test2"}]
p = Pipeline(workdir, jobs, 1, "torque", "NA", local=True)
p.start()
