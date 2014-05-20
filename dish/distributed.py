from logbook import NestedSetup, Logger, FileHandler
from dish.logging.zmqextras import ZeroMQPushHandler
import os

from IPython.utils.pickleutil import can_map
from types import FunctionType


def logging_wrapper(job, f, ip, port):
    """Wrapper to execute user passed functions remotely after
    setting up logging

    ip and port should specify somewhere we can push logging messages
    over zmq and have something useful happen to them
    """
    handler = NestedSetup([
        ZeroMQPushHandler("tcp://" + ip + ":" + port, level="DEBUG"),
        FileHandler(os.path.join(job["workdir"], job["description"]+".log"),
                    level="DEBUG", bubble=True)
    ])
    logger = Logger(job["description"])
    with handler.applicationbound():
        try:
            if job.get("tmpdir"):
                os.chdir(job["tmpdir"])
            else:
                os.chdir(job["workdir"])
            f(job, logger=logger)
        except:
            logger.exception("Task failed with traceback:")
            raise
        return job


def use_cloudpickle():
    """use cloudpickle to expand serialization support

    This is the same things as IPython's pickleutils.use_dill
    but for cloudpickle.
    """
    from cloud.serialization import cloudpickle

    global pickle
    pickle = cloudpickle

    try:
        from IPython.kernel.zmq import serialize
    except ImportError:
        pass
    else:
        serialize.pickle = cloudpickle

    # disable special function handling, let cloudpickle take care of it
    can_map.pop(FunctionType, None)
