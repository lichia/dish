from IPython.parallel import interactive, require

# import dish
# from dish import logging
# import logbook

@interactive
def wrapper(job):
    """Wrapper to execute user passed functions remotely after
    setting up logging
    """
    handler = NestedSetup([
        ZeroMQPushHandler("tcp://" + ip + ":" + port),
        FileHandler(os.path.join(job["workdir"], job["description"]+".log"),
                    level="DEBUG", bubble=True)
    ])
    logger = Logger(job["description"])
    with handler.applicationbound():
        f(job, logger=logger)
        return job
