"""Factory functions for producing functions we need to use in pipeline
methods, e.g. map"""

import subprocess


def cmdrunner(template, capture_in):
    def runner(job, logger):
    # TODO attach stdout of command to logging
        command = template.format(**job)
        to_log = "Running command `{}`".format(command)
        if capture_in:
            to_log += "Capturing output in job[{}]".format(capture_in)
        logger.info(to_log)
        output = subprocess.check_output(command, shell=True)
        logger.info(output)
        if capture_in:
            job[capture_in] = output
    return runner
