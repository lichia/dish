from IPython.parallel.error import CompositeError

import os

from nose.tools import assert_raises

from .test_pipeline import PipelineTest


class TestTransaction(PipelineTest):
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

    def test_transaction_handles_partial_failiures(self):
        """Transactions should be able to successfully copy the output of jobs
        that succeeded while skipping those that failed.
        """
        with open(os.path.join(self.p.jobs[0]["workdir"], "1"), "w") as f:
            f.write("hello")
        with assert_raises(CompositeError):
            with self.p.transaction("2"):
                self.p.run("cat {workdir}/1 > {tmpdir}/2")
        assert not os.path.exists(os.path.join(self.p.jobs[1]["workdir"], "2"))
        assert os.path.exists(os.path.join(self.p.jobs[0]["workdir"], "2"))

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
