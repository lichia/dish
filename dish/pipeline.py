import os
import tempfile
import shutil

from cluster_helper.cluster import cluster_view

from logbook import FileHandler, Logger
from dish.logging.zmqextras import ZeroMQPullSubscriber

from dish.distributed import logging_wrapper, use_cloudpickle
from dish import fs
from dish.factories import cmdrunner

from IPython.utils import localinterfaces

from random import randint
from contextlib import contextmanager


class Pipeline(object):
    """Represents the abstraction of a pipeline of jobs to be run
    distributed over machines
    """

    def __init__(self, workdir, jobs, total_cores, scheduler=None, queue=None,
                 local=False):
        """Initialize a pipeline.

        workdir: directory to use for scratch space and results. this
        needs to be visible to all nodes over NFS or similar

        jobs: a list of jobs, which are just dicts. the only required
        key for now is "description", which will be used for the
        directory that holds all this job's output

        total_cores: the total number of cores you have available for
        processing

        """

        # validate things
        for job in jobs:
            if type(job) is not dict:
                raise ValueError("job is not a dict: {}".format(job))
            if not job.get("description"):
                raise ValueError("job {} has not description".format(job))
        workdir = os.path.abspath(os.path.expanduser(workdir))
        if not os.path.exists(workdir):
            raise ValueError(
                "workdir: {} appears not to exist".format(workdir))
        self.workdir = workdir
        self.jobs = jobs
        self.total_cores = total_cores
        self.scheduler = scheduler
        self.queue = queue
        self.local = local
        # setup default cluster_view
        self._cluster_view = cluster_view


    def start(self):
        """Initialize workdir, logging, etc. in preparation for running jobs.
        """

        # make a working directory for each job
        for job in self.jobs:
            job["workdir"] = os.path.join(self.workdir, job["description"])
            fs.maybe_mkdir(job["workdir"])
        # temporary ipython profile directory
        self.ipythondir = os.path.join(self.workdir, ".ipython")
        fs.maybe_mkdir(self.ipythondir)
        # log dir
        self.logdir = os.path.join(self.workdir, "log")
        fs.maybe_mkdir(self.logdir)

        # determine which IP we are going to listen on for logging
        try:
            self.listen_ip = localinterfaces.public_ips()[0]
        except:
            raise ValueError("This machine appears not to have"
                             " any publicly visible IP addresses")

        # setup ZMQ logging
        self.handler = FileHandler(os.path.join(self.logdir, "dish.log"))
        self.listen_port = str(randint(5000, 10000))
        self.subscriber = ZeroMQPullSubscriber("tcp://" + self.listen_ip +
                                               ":" + self.listen_port)
        self.controller = self.subscriber.dispatch_in_background(self.handler)
        self.logger = Logger("dish_master")

    def stop(self):
        """Gracefully shutdown the Pipeline, cleaning up threads, sockets,
        etc.  Leaves working directory intact so everything can in
        principle be picked up again where we left off.

        """
        self.controller.stop()
        self.subscriber.close()

    def _compute_resources(self, cores_per_engine, mem_per_engine,
                           max_engines):
        if cores_per_engine > self.total_cores:
            raise ValueError("A job requested {0} but only {1}"
                             " are available.".format(cores_per_engine,
                                                      self.total_cores))
        num_engines = self.total_cores // cores_per_engine
        if max_engines:
            num_engines = min(num_engines, max_engines)
        # TODO in the future, should maybe validate that requested
        # cores and memory are actually going to be availible. This
        # would unfortunately have to be specialized for each
        # scheduler probably.
        return num_engines, cores_per_engine, mem_per_engine

    @contextmanager
    def group(self, cores=1, mem=None, max=None):
        # TODO this duplicates some code from p.map and is a bit
        # clunky, there is probably a better abstraction here
        engines, cores, mem = self._compute_resources(cores, mem, max)
        extra_params = {"run_local": self.local,
                        "cores": cores,
                        "mem": mem}
        old_view_factory = self._cluster_view
        cm = self._cluster_view(self.scheduler, self.queue,
                                engines, profile=self.ipythondir,
                                extra_params=extra_params)
        view = cm.gen.next()

        @contextmanager
        def reuse_view(*args, **kwargs):
            yield view

        # everything done in the block will use the view we just made
        self._cluster_view = reuse_view
        try:
            yield
        finally:
            # restore the normal cluster_view context manager on exit
            self._cluster_view = old_view_factory
            try:
                cm.gen.next()  # clean up the view we've been using
            except StopIteration:
                pass

    @contextmanager
    def transaction(self, targets):
        if type(targets) is not list:
            targets = [targets]
        to_run = []
        dont_run = []
        for job in self.jobs:
            if all((os.path.exists(target.format(**job))
                    for target in targets)):
                info = ("Skipping transaction for job {} targets {} "
                        "already present")
                fmt_targets = [target.format(**job) for target in targets]
                with self.handler.applicationbound():
                    self.logger.info(info.format(job["description"],
                                                 fmt_targets))
                dont_run.append(job)
            else:
                # targets not present for this job
                to_run.append(job)
        for job in to_run:
            job["tmpdir"] = tempfile.mkdtemp(dir=job["workdir"])
        self.jobs = to_run
        try:
            yield
            for job in self.jobs:
                fs.liftdir(job["tmpdir"], job["workdir"])
        finally:
            for job in self.jobs:
                shutil.rmtree(job["tmpdir"])
            self.jobs = dont_run + self.jobs

    def localmap(self, f):
        """Just like map, but work locally rather than launching an ipython
        cluster.  This is useful for tasks where the cluster launch
        overhead would swamp the cost of the actual work to be done.

        """
        self.jobs = map(logging_wrapper, self.jobs,
                        (f for j in self.jobs),
                        (self.listen_ip for j in self.jobs),
                        (self.listen_port for j in self.jobs))

    def map(self, f, cores=1, mem=None, max=None):
        """Map the function `f` over all of the `jobs` in this pipeline. `f`
        must be a function of two arguments, the job and a logger. It
        should modify the job it is passed, which will then be
        returned over the wire. A silly example:

        ```
        def f(job, logger):
            job["capitalized_description"] = job["description"].toupper()
        p.map(f)
        ```

        Will give each `job` in the pipeline a `capitalized_description`
        attribute, which can then be used in future pipline operations.

        `cores` and `mem` are used to specify the cores and memory
        required by this step; they will be passed to the underlying
        scheduler. `max` can be used as a hard limit on the number of
        jobs to run. This is useful if, for example, a particular task
        puts pressure on some sort of storage system (a distributed
        file system, object store, etc.) that you know will fail under
        too much load.

        """
        if not self.jobs:
            # this looks very odd, it's necessary because sometimes
            # being a transaction causes self.jobs to be empty, and
            # IPython throws errors if you try to make over the empty
            # list. It might be cleaner to catch the error after
            # letting IPython do the map; will have to think about it.
            return
        engines, cores, mem = self._compute_resources(cores, mem, max)
        extra_params = {"run_local": self.local,
                        "cores": cores,
                        "mem": mem}
        with self._cluster_view(self.scheduler, self.queue,
                                engines, profile=self.ipythondir,
                                extra_params=extra_params) as view:
            # using cloudpickle allows us to serialize all sorts of things
            # we wouldn't otherwise be able to
            dview = view.client.direct_view()
            use_cloudpickle()
            dview.apply(use_cloudpickle)
            self.jobs = view.map_sync(logging_wrapper, self.jobs,
                                      (f for j in self.jobs),
                                      (self.listen_ip for j in self.jobs),
                                      (self.listen_port for j in self.jobs))

    def run(self, template, capture_in=None, **kwargs):
        """Run the `template` formatted with the contents of each job. Example:

        ```
        p.run("touch {workdir}/example.txt")
        ```

        will make an example.txt file in each job's workdir.

        `cores` and `mem` mean the same thing they do in the `map` method.

        If a string is passed for `capture_in`, the stdout of the command
        will be captured in `job[capture_in]` for each job.

        """
        runner = cmdrunner(template, capture_in)
        self.map(runner, **kwargs)
