import os

from logbook import FileHandler
from logbook.queues import ZeroMQSubscriber

from IPython.utils import localinterfaces

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
        to commands.  Keys should be names of commands, values should also
        be dicts, each with two keys: "cores", and "mem".

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
        if not localinterfaces.public_ips():
            raise ValueError("This machine appears not to have"
                             " any publicly visible IP addresses")
        self.listen_ip = localinterfaces.public_ips()[0]

        # setup ZMQ logging
        handler = FileHandler(os.path.join(self.logdir, "log.txt"))
        # TODO figure out a reasonable way to determine port
        self.subscriber = ZeroMQSubscriber("tcp://" + self.listen_ip + ":9090")
        self.controller = self.subscriber.dispatch_in_background(handler)
