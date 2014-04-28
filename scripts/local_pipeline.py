from cluster_helper import cluster
import tempfile
import os
from dish.pipeline import Pipeline

if os.uname()[0] == 'Darwin':
    # workaround for stupid OSX file handle limits
    # see: https://github.com/roryk/ipython-cluster-helper/issues/18
    cluster.cluster_cmd_argv = [s.replace("50000", "2000") for s
                                in cluster.cluster_cmd_argv]


workdir = tempfile.mkdtemp()
os.chdir(workdir)
jobs = [{"description": "test1"}, {"description": "test2"}]
p = Pipeline(workdir, jobs, 1, "torque", "NA", local=True)
p.start()
