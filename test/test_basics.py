import os
import re

from .test_pipeline import PipelineTest


class TestBasics(PipelineTest):

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

    def test_should_run_in_job_workdir(self):
        """Everything should be run in the workdir of the correct job."""
        self.p.run("touch test")
        for job in self.p.jobs:
            assert os.path.exists(os.path.join(job["workdir"], "test"))

    def test_retries(self):
        """Passing a high number for retries should enable fault tolerance."""
        # TODO this still fails in test due to the fact that IPython
        # will not resubmit a task to the same engine, and we only
        # have one engine in local testing. ref
        # https://github.com/ipython/ipython/issues/5977

        self.p.retries = 2

        def fails_at_first(job, logger):
            if not os.path.exists(os.path.join(job["workdir"], "here")):
                open(os.path.join(job["workdir"], "here"), "a").close()
                raise RuntimeError("fail!")
            else:
                job["worked"] = True
        self.p.map(fails_at_first)
        for job in self.p.jobs:
            assert job["worked"]
