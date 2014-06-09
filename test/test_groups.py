from nose.tools import assert_raises

from IPython.parallel.error import CompositeError

from .test_pipeline import PipelineTest


class TestGroups(PipelineTest):
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
