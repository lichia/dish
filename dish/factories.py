"""Factory functions for producing functions we need to use in pipeline
methods, e.g. map"""

from subprocess import PIPE, CalledProcessError, Popen


def check_output_and_error(cmd, shell=True):
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=shell)
    stdout, stderr = proc.communicate()
    if proc.returncode != 0:
        raise CalledProcessError(proc.returncode, cmd)
    return stdout, stderr


def cmdrunner(template, capture_in):
    def runner(job, logger):
    # TODO make stdout logged line-by-line, like bcbio
        command = template.format(**job)
        to_log = "Running command `{}`".format(command)
        if capture_in:
            to_log += "Capturing output in job[{}]".format(capture_in)
        logger.info(to_log)
        stdout, stderr = check_output_and_error(command)
        stripped_out = stdout.rstrip()
        stripped_err = stderr.rstrip()
        if stripped_out:  # don't log empty line if there's no output
            logger.info(stripped_out)
        if stripped_err:
            logger.debug(stripped_err)
        if capture_in:
            job[capture_in] = stdout  # TODO maybe strip here?
    return runner
