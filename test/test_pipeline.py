from dish.pipeline import Pipeline
from IPython.parallel import interactive

import tempfile
import shutil
import os
from cluster_helper import cluster

import re

if os.uname()[0] == 'Darwin':
    # workaround for stupid OSX file handle limits
    # see: https://github.com/roryk/ipython-cluster-helper/issues/18
    cluster.cluster_cmd_argv = [s.replace("50000", "2000") for s
                                in cluster.cluster_cmd_argv]


class TestPipeline(object):
    """Integration test that Pipelines work with spinning up a cluster."""

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

    def test_localmap(self):
        """Test that localmapping works just like mapping
        with a cluster."""
        def trivial(job, logger):
            job["test"] = "test"
        self.p.localmap(trivial)
        for job in self.p.jobs:
            assert job["test"] == "test"

    def test_basic_map(self):
        """Test distributed mapping."""
        def trivial(job, logger):
            job["test"] = "test"
        self.p.map(trivial)
        for job in self.p.jobs:
            assert job["test"] == "test"

    def test_map_with_module(self):
        """You should be able to use imported modules in remote function
        calls.

        """
        def silly_regex(job, logger):
            # TODO for some reason we still can't pickle methods so attaching
            # a regex match object doesn't work here
            job["test"] = bool(re.search("test", job["description"]))
        self.p.map(silly_regex)
        for job in self.p.jobs:
            assert job["test"]  # should be True

    def test_run(self):
        """Test that running commands works."""
        self.p.run("touch {workdir}/test.txt")
        for job in self.p.jobs:
            assert os.path.exists(os.path.join(job["workdir"], "test.txt"))

    def test_capturing_output(self):
        """Test capturing output of commands."""
        self.p.run("echo hello", capture_in="output")
        for job in self.p.jobs:
            assert job["output"] == "hello\n"
