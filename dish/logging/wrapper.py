from IPython.parallel import interactive, require

@interactive
def _wrapper(job):
    """Wrapper to execute user passed functions remotely after
    setting up logging

    All of the dependancies (NestedSetup, ZeroMQHandler, ip etc.)
    are loaded by the Pipeline before this is executed.
    """
    handler = NestedSetup([
        ZeroMQPushHandler("tcp://" + ip + ":" + port),
        FileHandler(os.path.join(job["workdir"], job["description"]+".log"),
                    level="DEBUG", bubble=True)
    ])
    logger = Logger(job["description"])
    with handler.applicationbound():
        f(job, logger=logger)
        # TODO cleanup zmq stuff
        return job
