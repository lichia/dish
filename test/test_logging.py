from IPython.parallel.error import CompositeError

import os

from nose.tools import assert_raises

from .utils import assert_eventually_equal
from .test_pipeline import PipelineTest


class TestLogging(PipelineTest):
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

    def test_stdout_is_logged(self):
        """p.run should log stdout of the command."""
        self.p.run("echo testing123")
        pipeline_log = open(os.path.join(self.p.logdir, "dish.log")).read()
        assert_eventually_equal("testing123" in pipeline_log, True)
        for job in self.p.jobs:
            job_log = open(os.path.join(job["workdir"],
                                        job["description"]+".log")).read()
            assert_eventually_equal("testing123" in job_log, True)

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
