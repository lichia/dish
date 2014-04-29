from dish.pipeline import Pipeline
from IPython.parallel import interactive
from IPython.parallel.error import CompositeError

import tempfile
import shutil
import os
from cluster_helper import cluster

import re

from nose.tools import assert_raises

if os.uname()[0] == 'Darwin':
    # workaround for stupid OSX file handle limits
    # see: https://github.com/roryk/ipython-cluster-helper/issues/18
    cluster.cluster_cmd_argv = [s.replace("50000", "2000") for s
                                in cluster.cluster_cmd_argv]


class TestPipeline(object):
    """Integration test that Pipelines work with spinning up a cluster."""

    def setup(self):
        self.workdir = tempfile.mkdtemp()
        self.tmpdir = tempfile.mkdtemp()
        os.chdir(self.tmpdir)
        jobs = [{"description": "test1"}, {"description": "test2"}]
        self.p = Pipeline(self.workdir, jobs, 1, "torque", "NA", local=True)
        self.p.start()

    def teardown(self):
        self.p.stop()
        shutil.rmtree(self.workdir)
        shutil.rmtree(self.tmpdir)

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

    def test_logging(self):
        """Test that logging information is propagated and stored
        correctly.
        """
        def logs_things(job, logger):
            logger.info(job["description"]+"loggingtest")
        self.p.map(logs_things)
        # TODO abstract out this logging testing stuff
        pipeline_log = open(os.path.join(self.p.logdir, "dish.log")).read()
        for job in self.p.jobs:
            job_log = open(os.path.join(job["workdir"],
                                        job["description"]+".log")).read()
            assert job["description"]+"loggingtest" in job_log
            assert job["description"]+"loggingtest" in pipeline_log

    def test_logging_gets_traceback(self):
        """When a call fails, we should log traceback info."""
        def failing(job, logger):
            raise RuntimeError(job["description"]+"error")
        with assert_raises(CompositeError):
            self.p.map(failing)
        pipeline_log = open(os.path.join(self.p.logdir, "dish.log")).read()
        for job in self.p.jobs:
            job_log = open(os.path.join(job["workdir"],
                                        job["description"]+".log")).read()
            assert job["description"]+"error" in job_log
            assert job["description"]+"error" in pipeline_log

    def test_groups(self):
        """Test that groups work."""
        def inc(job, logger):
            if job.get("test"):
                job["test"] += 1
            else:
                job["test"] = 1
        with self.p.group():
            self.p.map(inc)
            self.p.map(inc)
        for job in self.p.jobs:
            assert job["test"] == 2

    def test_groups_with_error(self):
        """Regression test that groups that throw errors correctly restore the
        _cluster_view context manager.

        """

        def fails(job, logger):
            raise RuntimeError("fail")

        def trivial(job, logger):
            job["test"] = "test"

        with self.p.group():
            with assert_raises(CompositeError):
                self.p.map(fails)
        self.p.map(trivial)
        for job in self.p.jobs:
            assert job["test"] == "test"
