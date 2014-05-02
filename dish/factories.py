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
        stripped = output.rstrip()
        if stripped:  # don't log empty line if there's no output
            logger.info(stripped)
        if capture_in:
            job[capture_in] = output
    return runner
