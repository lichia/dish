from dish.pipeline import Pipeline
from IPython.parallel import interactive

import tempfile
import shutil
import os
from cluster_helper import cluster

if os.uname()[0] == 'Darwin':
    # workaround for stupid OSX file handle limits
    # see: https://github.com/roryk/ipython-cluster-helper/issues/18
    cluster.cluster_cmd_argv = [s.replace("50000", "2000") for s
                                in cluster.cluster_cmd_argv]


class TestPipeline(object):
    """Integration test that Pipelines work."""

    @classmethod
    def setup_class(cls):
        cls.workdir = tempfile.mkdtemp()
        cls.tmpdir = tempfile.mkdtemp()
        os.chdir(cls.tmpdir)
        jobs = [{"description": "test1"}, {"description": "test2"}]
        cls.p = Pipeline(cls.workdir, jobs, 1, "torque", "NA", local=True)
        cls.p.start()

    @classmethod
    def teardown_class(cls):
        cls.p.stop()
        shutil.rmtree(cls.workdir)
        shutil.rmtree(cls.tmpdir)

    def test_start_is_idempotent(self):
        """Starting a pipeline should be idempotent
        so we can pick up again after failiures."""
        before = os.listdir(self.p.workdir)
        self.p.start()
        after = os.listdir(self.p.workdir)
        assert before == after

    def test_function_call(self):
        """Test distributed function calls."""
        @interactive
        def trivial(job, logger):
            job["test"] = "test"
        self.p.call(trivial)
        for job in self.p.jobs:
            assert job["test"] == "test"
