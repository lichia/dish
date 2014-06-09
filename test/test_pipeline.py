from dish.pipeline import Pipeline
from IPython.parallel.error import CompositeError

import tempfile
import shutil
import os

import re

from nose.tools import assert_raises

from .utils import mock_view, assert_eventually_equal

# TODO figure out a good way to determine this at runtime
# I really don't want to have to write a nose plugin -_____-
MOCK_CLUSTER = True


class TestPipeline(object):
    """Integration test that Pipelines work with spinning up a cluster."""

    def setup(self):
        self.workdir = tempfile.mkdtemp()
        self.tmpdir = tempfile.mkdtemp()
        os.chdir(self.tmpdir)
        jobs = [{"description": "test1"}, {"description": "test2"}]
        self.p = Pipeline(self.workdir, jobs, 1, "torque", "NA", local=True)
        if MOCK_CLUSTER:
            self.p._cluster_view = mock_view
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

    def test_transaction_works(self):
        """Test that running commands transactionally works as expected."""
        old_jobs = self.p.jobs
        with self.p.transaction("{workdir}/A"):
            self.p.run("touch {tmpdir}/A")
        new_jobs = self.p.jobs
        assert new_jobs == old_jobs
        for job in self.p.jobs:
            assert os.path.exists(os.path.join(job["workdir"], "A"))

    def test_transaction_skips(self):
        """Test that running commands idempotently skips if targets already
        exist.
        """
        self.p.run("touch {workdir}/A")
        with self.p.transaction("{workdir}/A"):
            self.p.run("touch {tmpdir}/B")
        pipeline_log = open(os.path.join(self.p.logdir, "dish.log")).read()
        assert "Skipping" in pipeline_log
        for job in self.p.jobs:
            assert not os.path.exists(os.path.join(job["workdir"], "B"))

    def test_transaction_doesnt_copy_on_failiure(self):
        """Transactions should not copy anything over in the case
        that an error is raised"""
        def errors(job, logger):
            raise RuntimeError("test error")
        with assert_raises(CompositeError):
            with self.p.transaction("{workdir}/A"):
                self.p.run("touch {tmpdir}/A")
                self.p.map(errors)
        for job in self.p.jobs:
            assert not os.path.exists(os.path.join(job["workdir"], "A"))

    def test_stdout_is_logged(self):
        """p.run should log stdout of the command."""
        self.p.run("echo testing123")
        pipeline_log = open(os.path.join(self.p.logdir, "dish.log")).read()
        assert_eventually_equal("testing123" in pipeline_log, True)
        for job in self.p.jobs:
            job_log = open(os.path.join(job["workdir"],
                                        job["description"]+".log")).read()
            assert_eventually_equal("testing123" in job_log, True)

    def test_should_run_in_job_workdir(self):
        """Everything should be run in the workdir of the correct job."""
        self.p.run("touch test")
        for job in self.p.jobs:
            assert os.path.exists(os.path.join(job["workdir"], "test"))

    def test_transaction_targets_should_be_relative_to_workdir(self):
        """Transaction targets should be specifiable relative to workdir."""
        self.p.run("touch A")
        with self.p.transaction("A"):
            self.p.run("touch B")
        for job in self.p.jobs:
            assert not os.path.exists(os.path.join(job["workdir"], "B"))

    def test_transaction_targets_should_be_made_in_tmpdir_first(self):
        """Transaction targets should not be created in the workdir when paths
        are relative.
        """
        with assert_raises(CompositeError):
            with self.p.transaction("A"):
                self.p.run("touch B")
                self.p.run("i_dont_exist")  # so that transaction fails
        for job in self.p.jobs:
            assert not os.path.exists(os.path.join(job["workdir"], "B"))
