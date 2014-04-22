
import os

from cluster_helper.cluster import cluster_view

from logbook import FileHandler, NestedSetup, Logger
from logbook.queues import ZeroMQSubscriber, ZeroMQHandler

from IPython.utils import localinterfaces
from IPython.parallel import require


class Pipeline(object):
    """Represents the abstraction of a pipeline of jobs to be run
    distributed over machines
    """

    def __init__(self, workdir, jobs, resources, total_cores, scheduler, queue):
        """Initialize a pipeline.

        workdir: directory to use for scratch space and results. this
        needs to be visible to all nodes over NFS or similar

        jobs: a list of jobs, which are just dicts. the only required
        key for now is "description", which will be used for the
        directory that holds all this job's output

        resources: a dict that describes the resources to be allocated
        to commands.  Keys should be names of commands, values should
        also be dicts, each with at least two keys: "cores", and
        "mem". You can optional also specifiy max_jobs to limit the
        parallelism at a given stage. This is useful e.g. if you know
        that something is I/O intensive and will overwhelm the some
        storage system if too many are run at once.

        """

        # validate things
        for job in jobs:
            if type(job) is not dict:
                raise ValueError("job is not a dict: {}".format(job))
        for spec in resources:
            if (type(spec) is not dict or spec.get("mem") is None or spec.get("cores") is None):
                raise ValueError("resource spec appears malformed: {}".format(spec))
        workdir = os.path.abspath(os.path.expanduser(workdir))
        if not os.path.exists(workdir):
            raise ValueError("workdir: {} appears not to exist".format(workdir))
        self.workdir = workdir
        self.resources = resources
        self.jobs = jobs
        self.total_cores = total_cores
        self.scheduler = scheduler
        self.queue = queue

    def start(self):
        """Initialize workdir, logging, etc. in preparation for running jobs.
        """

        # make a working directory for each job
        for job in self.jobs:
            job["workdir"] = os.path.join(self.workdir, job["description"])
            os.mkdir(job["workdir"])
        # temporary ipython profile directory
        self.ipythondir = os.path.join(self.workdir, ".ipython")
        os.mkdir(self.ipythondir)
        # log dir
        self.logdir = os.path.join(self.workdir, "log")
        os.mkdir(self.logdir)
        # place to keep completion info
        self.progress_store = os.path.join(self.workdir, ".progress")

        # determine which IP we are going to listen on for logging
        try:
            self.listen_ip = localinterfaces.public_ips()[0]
        except:
            raise ValueError("This machine appears not to have"
                             " any publicly visible IP addresses")

        # setup ZMQ logging
        handler = FileHandler(os.path.join(self.logdir, "dish.log"))
        # TODO figure out a reasonable way to choose port
        self.subscriber = ZeroMQSubscriber("tcp://" + self.listen_ip + ":9090")
        self.controller = self.subscriber.dispatch_in_background(handler)
        self.stages = []

    def wrap(self, f):
        """ Wrap a callable so that logging is setup correctly before calling it.
        """
        @require(NestedSetup, FileHandler, ZeroMQHander, Logger, f, ip=self.listen_ip)
        def wrapper(job):
            handler = NestedSetup([
                ZeroMQHandler('tcp://' + ip + ':9090', level="INFO"),
                FileHandler(os.path.join(job.workdir, job.description+".log"),
                            level="DEBUG", bubble=True),
            ])
            with handler.applicationbound():
                logger = Logger(job.description)
                f(job)
                return job
        return wrapper

    def call(self, f, mem=None, cores=None):
        """Call the function `f`. It will be wrapped for logging and then
        passed each `job` that it is being called on.

        """
        wrapped = self.wrap(f)
        with cluster_view(scheduler=self.scheduler, queue=self.queue,
                          extra_params={"cores": cores, "mem": mem},
                          profile=self.ipythondir) as view:
            self.jobs = view.map_sync(wrapped, jobs)
