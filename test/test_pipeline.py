from dish.pipeline import Pipeline

import tempfile
import shutil
import os

from .utils import mock_view

# TODO figure out a good way to determine this at runtime
# I really don't want to have to write a nose plugin -_____-
MOCK_CLUSTER = True


class PipelineTest(object):
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
